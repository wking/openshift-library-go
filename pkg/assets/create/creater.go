package create

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ghodss/yaml"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	"github.com/openshift/library-go/pkg/assets"
)

// CreateOptions allow to specify additional create options.
type CreateOptions struct {
	// Filters allows to filter which files we will read from disk.
	// Multiple filters can be specified, in that case only files matching all filters will be returned.
	Filters []assets.FileInfoPredicate
}

// EnsureManifestsCreated ensures that all resource manifests from the specified directory are created.
// This function will keep retrying creation until no errors are reported.
// Pass the context to indicate how much time you are willing to wait until all resources are created.
func EnsureManifestsCreated(ctx context.Context, manifestDir string, restConfig *rest.Config, options CreateOptions) error {
	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	dc, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return err
	}

	manifests, err := load(manifestDir, options)
	if err != nil {
		return err
	}

	getRESTMapper := func() (meta.RESTMapper, error) {
		gr, err := restmapper.GetAPIGroupResources(dc)
		if err != nil {
			return nil, err
		}
		return restmapper.NewDiscoveryRESTMapper(gr), nil
	}

	mapper, err := getRESTMapper()
	if err != nil {
		return err
	}

	// Retry creation until no errors are returned or the timeout is hit.
	var lastCreateError error
	err = wait.PollImmediateUntil(500*time.Millisecond, func() (bool, error) {
		err, refresh := create(manifests, client, mapper)
		if err == nil {
			// No errors means all resources were created successfully, reset lastCreateError and exit the loop.
			lastCreateError = nil
			return true, nil
		}
		// We got rest-mapper error, force to refresh discovery so next create use updated mapper.
		// TODO: This is expensive and will probably take couple seconds to get updated discovery back from the API server.
		if refresh {
			mapper, err = getRESTMapper()
			if err != nil {
				return false, err
			}
		}
		lastCreateError = err
		return false, nil
	}, ctx.Done())

	// Return the last observed set of errors from the create process instead of timeout error.
	if lastCreateError != nil {
		return lastCreateError
	}

	return err
}

func create(manifests map[string]*unstructured.Unstructured, client dynamic.Interface, mapper meta.RESTMapper) (error, bool) {
	// Sort all manifests, so in case they use number prefixes, we follow their order, which will increase
	// the chances for the create loop to succeed faster.
	sortedManifestPaths := []string{}
	for key := range manifests {
		sortedManifestPaths = append(sortedManifestPaths, key)
	}
	sort.Strings(sortedManifestPaths)

	// Record all errors for the given manifest path (so when we report errors, users can see what manifest failed).
	errs := map[string]error{}

	// In case we fail to find a rest-mapping for the resource, force to fetch the updated discovery on next run.
	reloadDiscovery := false

	for _, path := range sortedManifestPaths {
		gvk := manifests[path].GetObjectKind().GroupVersionKind()
		mappings, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			errs[path] = fmt.Errorf("unable to get REST mapping: %v", err)
			reloadDiscovery = true
			continue
		}

		if mappings.Scope.Name() == meta.RESTScopeNameRoot {
			_, err = client.Resource(mappings.Resource).Create(manifests[path], metav1.CreateOptions{})
		} else {
			_, err = client.Resource(mappings.Resource).Namespace(manifests[path].GetNamespace()).Create(manifests[path], metav1.CreateOptions{})
		}

		// Resource already exists means we already succeeded
		// This should never happen as we remove already created items from the manifest list, unless the resource existed beforehand.
		if kerrors.IsAlreadyExists(err) {
			delete(manifests, path)
			continue
		}

		if err != nil {
			errs[path] = fmt.Errorf("failed to create: %v", err)
			continue
		}

		// Creation succeeded lets remove the manifest from the list to avoid creating it second time
		delete(manifests, path)
	}

	return formatErrors(errs), reloadDiscovery
}

func formatErrors(errors map[string]error) error {
	if len(errors) == 0 {
		return nil
	}
	aggregatedErrMessages := []string{}
	keys := []string{}
	for key := range errors {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, k := range keys {
		aggregatedErrMessages = append(aggregatedErrMessages, fmt.Sprintf("%q: %v", k, errors[k]))
	}
	return fmt.Errorf("failed to create some manifests:\n%s\n", strings.Join(aggregatedErrMessages, "\n"))
}

func load(assetsDir string, options CreateOptions) (map[string]*unstructured.Unstructured, error) {
	if _, err := os.Stat(assetsDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("directory %q does not exists", assetsDir)
	}

	manifests := map[string]*unstructured.Unstructured{}
	manifestsBytesMap, err := assets.LoadFilesRecursively(assetsDir, options.Filters...)
	if err != nil {
		return nil, err
	}

	errs := map[string]error{}
	for manifestPath, manifestBytes := range manifestsBytesMap {
		manifestJSON, err := yaml.YAMLToJSON(manifestBytes)
		if err != nil {
			errs[manifestPath] = fmt.Errorf("unable to convert asset %q from YAML to JSON: %v", manifestPath, err)
			continue
		}
		manifestObj, err := runtime.Decode(unstructured.UnstructuredJSONScheme, manifestJSON)
		if err != nil {
			errs[manifestPath] = fmt.Errorf("unable to decode asset %q: %v", manifestPath, err)
			continue
		}
		manifestUnstructured, ok := manifestObj.(*unstructured.Unstructured)
		if !ok {
			errs[manifestPath] = fmt.Errorf("unable to convert asset %q to unstructed", manifestPath)
			continue
		}
		manifests[manifestPath] = manifestUnstructured
	}

	return manifests, formatErrors(errs)
}
