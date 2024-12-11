package kustomize

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"runtime"
	"sort"
	"strings"

	k8scorev1 "k8s.io/api/core/v1"
	k8svalidation "k8s.io/apimachinery/pkg/api/validation"
	k8sunstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"k8s.io/apimachinery/pkg/util/jsonmergepatch"
	"k8s.io/apimachinery/pkg/util/mergepatch"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/kubectl/pkg/scheme"
)

const lastAppliedConfigAnnotation = k8scorev1.LastAppliedConfigAnnotation
const gzipLastAppliedConfigAnnotation = "kustomization.kubestack.com/last-applied-config-gzip"

func setLastAppliedConfig(km *kManifest, gzipLastAppliedConfig bool) {
	annotations := km.resource.GetAnnotations()
	if len(annotations) == 0 {
		annotations = make(map[string]string)
	}

	annotations[lastAppliedConfigAnnotation] = string(km.json)

	if gzipLastAppliedConfig {
		needsGzip := false
		sErr := k8svalidation.ValidateAnnotationsSize(annotations)
		if sErr != nil {
			needsGzip = true
		}

		if needsGzip {
			var buf bytes.Buffer
			zw := gzip.NewWriter(&buf)

			_, err1 := zw.Write(km.json)

			err2 := zw.Close()

			if err1 == nil && err2 == nil {
				annotations[gzipLastAppliedConfigAnnotation] = base64.StdEncoding.EncodeToString(buf.Bytes())
				delete(annotations, lastAppliedConfigAnnotation)
			}
		}
	}

	km.resource.SetAnnotations(annotations)
	km.json, _ = km.resource.MarshalJSON()
}

func getLastAppliedConfig(u *k8sunstructured.Unstructured, gzipLastAppliedConfig bool) (lac string) {
	annotations := u.GetAnnotations()

	lac = u.GetAnnotations()[lastAppliedConfigAnnotation]

	if gzipLastAppliedConfig {
		// read the compressed lac if available
		if gzEnc, ok := annotations[gzipLastAppliedConfigAnnotation]; ok {
			gzDec, err := base64.StdEncoding.DecodeString(gzEnc)
			if err != nil {
				log.Fatal(err)
			}

			var buf bytes.Buffer
			buf.Write(gzDec)

			zr, err1 := gzip.NewReader(&buf)

			lacBuf := new(strings.Builder)
			_, err2 := io.Copy(lacBuf, zr)

			err3 := zr.Close()

			// in case of any error, fall back to the uncompressed lac
			if err1 == nil && err2 == nil && err3 == nil {
				lac = lacBuf.String()
			}
		}
	}

	return strings.TrimRight(lac, "\r\n")
}

func getPatch(gvk k8sschema.GroupVersionKind, original []byte, modified []byte, current []byte) (pt k8stypes.PatchType, p []byte, err error) {
	versionedObject, err := scheme.Scheme.New(gvk)
	switch {
	case k8sruntime.IsNotRegisteredError(err):
		pt = k8stypes.MergePatchType

		preconditions := []mergepatch.PreconditionFunc{
			mergepatch.RequireKeyUnchanged("kind"),
			mergepatch.RequireMetadataKeyUnchanged("name"),
		}

		p, err = jsonmergepatch.CreateThreeWayJSONMergePatch(original, modified, current, preconditions...)
		if err != nil {
			return pt, p, fmt.Errorf("getPatch failed: %s", err)
		}
	case err != nil:
		return pt, p, fmt.Errorf("getPatch failed: %s", err)
	case err == nil:
		pt = k8stypes.StrategicMergePatchType

		lookupPatchMeta, err := strategicpatch.NewPatchMetaFromStruct(versionedObject)
		if err != nil {
			return pt, p, fmt.Errorf("getPatch failed: %s", err)
		}

		p, err = strategicpatch.CreateThreeWayMergePatch(original, modified, current, lookupPatchMeta, true)
		if err != nil {
			return pt, p, fmt.Errorf("getPatch failed: %s", err)
		}
	}

	return pt, p, nil
}

// log error including caller name
func logError(m error) error {
	pc, _, _, _ := runtime.Caller(1)
	fn := runtime.FuncForPC(pc)

	return fmt.Errorf("%s: %s", fn.Name(), m)
}

func getLiveManifestFields_WithIgnoredFields(ignoredFields []string, userProvided *kManifest, liveManifest *kManifest) string {

	flattenedUser := Flatten(userProvided.resource.Object)
	flattenedLive := Flatten(liveManifest.resource.Object)

	// remove any fields from the user provided set or control fields that we want to ignore
	fieldsToTrim := append([]string(nil), kubernetesControlFields...)
	if len(ignoredFields) > 0 {
		fieldsToTrim = append(fieldsToTrim, ignoredFields...)
	}

	for _, field := range fieldsToTrim {
		delete(flattenedUser, field)

		// check for any nested fields to ignore
		for k := range flattenedUser {
			if strings.HasPrefix(k, field+".") {
				delete(flattenedUser, k)
			}
		}
	}

	// update the user provided flattened string with the live versions of the keys
	// this implicitly excludes anything that the user didn't provide as it was added by kubernetes runtime (annotations/mutations etc)
	var userKeys []string
	for userKey, userValue := range flattenedUser {
		normalizedUserValue := strings.TrimSpace(userValue)

		// only include the value if it exists in the live version
		// that is, don't add to the userKeys array unless the key still exists in the live manifest
		if _, exists := flattenedLive[userKey]; exists {
			userKeys = append(userKeys, userKey)
			normalizedLiveValue := strings.TrimSpace(flattenedLive[userKey])
			flattenedUser[userKey] = normalizedLiveValue
			if normalizedUserValue != normalizedLiveValue {
				log.Printf("[TRACE] yaml drift detected in %s for %s, was: %s now: %s", userProvided.GetSelfLink(), userKey, normalizedUserValue, normalizedLiveValue)
			}
		} else {
			if normalizedUserValue != "" {
				log.Printf("[TRACE] yaml drift detected in %s for %s, was %s now blank", userProvided.GetSelfLink(), userKey, normalizedUserValue)
			}
		}
	}

	sort.Strings(userKeys)
	var returnedValues []string
	for _, k := range userKeys {
		returnedValues = append(returnedValues, fmt.Sprintf("%s=%s", k, flattenedUser[k]))
	}

	return strings.Join(returnedValues, ",")
}

var kubernetesControlFields = []string{
	"status",
	"metadata.finalizers",
	"metadata.initializers",
	"metadata.ownerReferences",
	"metadata.creationTimestamp",
	"metadata.generation",
	"metadata.resourceVersion",
	"metadata.uid",
	"metadata.annotations.kubectl.kubernetes.io/last-applied-configuration",
	"metadata.managedFields",
}
