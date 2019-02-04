package create

import (
	"os"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/restmapper"
	ktesting "k8s.io/client-go/testing"

	"github.com/openshift/library-go/pkg/assets"
)

var resources = []*restmapper.APIGroupResources{
	{
		Group: metav1.APIGroup{
			Name: "kubeapiserver.operator.openshift.io",
			Versions: []metav1.GroupVersionForDiscovery{
				{Version: "v1alpha1"},
			},
			PreferredVersion: metav1.GroupVersionForDiscovery{Version: "v1alpha1"},
		},
		VersionedResources: map[string][]metav1.APIResource{
			"v1alpha1": {
				{Name: "kubeapiserveroperatorconfigs", Namespaced: false, Kind: "KubeAPIServerOperatorConfig"},
			},
		},
	},
	{
		Group: metav1.APIGroup{
			Name: "apiextensions.k8s.io",
			Versions: []metav1.GroupVersionForDiscovery{
				{Version: "v1beta1"},
			},
			PreferredVersion: metav1.GroupVersionForDiscovery{Version: "v1beta1"},
		},
		VersionedResources: map[string][]metav1.APIResource{
			"v1beta1": {
				{Name: "customresourcedefinitions", Namespaced: false, Kind: "CustomResourceDefinition"},
			},
		},
	},
	{
		Group: metav1.APIGroup{
			Name: "",
			Versions: []metav1.GroupVersionForDiscovery{
				{Version: "v1"},
			},
			PreferredVersion: metav1.GroupVersionForDiscovery{Version: "v1"},
		},
		VersionedResources: map[string][]metav1.APIResource{
			"v1": {
				{Name: "namespaces", Namespaced: false, Kind: "Namespace"},
				{Name: "configmaps", Namespaced: true, Kind: "ConfigMap"},
				{Name: "secrets", Namespaced: true, Kind: "Secret"},
			},
		},
	},
}

func TestCreate(t *testing.T) {
	resourcesWithoutKubeAPIServer := resources[1:]
	testConfigMap := &unstructured.Unstructured{}
	testConfigMap.SetGroupVersionKind(schema.GroupVersionKind{
		Version: "v1",
		Kind:    "ConfigMap",
	})
	testConfigMap.SetName("aggregator-client-ca")
	testConfigMap.SetNamespace("openshift-kube-apiserver")

	tests := []struct {
		name              string
		discovery         []*restmapper.APIGroupResources
		expectError       bool
		expectFailedCount int
		expectReload      bool
		existingObjects   []runtime.Object
		evalActions       func(*testing.T, []ktesting.Action)
	}{
		{
			name:      "create all resources",
			discovery: resources,
		},
		{
			name:              "fail to create kube apiserver operator config",
			discovery:         resourcesWithoutKubeAPIServer,
			expectFailedCount: 1,
			expectError:       true,
			expectReload:      true,
		},
		{
			name:            "create all resources",
			discovery:       resources,
			existingObjects: []runtime.Object{testConfigMap},
		},
	}

	fakeScheme := runtime.NewScheme()
	// TODO: This is a workaround for dynamic fake client bug where the List kind is enforced and duplicated in object reactor.
	fakeScheme.AddKnownTypeWithName(schema.GroupVersionKind{Version: "v1", Kind: "ListList"}, &unstructured.UnstructuredList{})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			manifests, err := load("testdata", CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}

			dynamicClient := dynamicfake.NewSimpleDynamicClient(fakeScheme, tc.existingObjects...)
			restMapper := restmapper.NewDiscoveryRESTMapper(tc.discovery)

			err, reload := create(manifests, dynamicClient, restMapper)
			if tc.expectError && err == nil {
				t.Errorf("expected error, got no error")
				return
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tc.expectReload && !reload {
				t.Errorf("expected reload, got none")
				return
			}
			if !tc.expectReload && reload {
				t.Errorf("unexpected reload, got one")
				return
			}
			if len(manifests) != tc.expectFailedCount {
				t.Errorf("expected %d failed manifests, got %d", tc.expectFailedCount, len(manifests))
				return
			}
			if tc.evalActions != nil {
				tc.evalActions(t, dynamicClient.Actions())
			}
		})

	}
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name                  string
		options               CreateOptions
		assetDir              string
		expectedManifestCount int
		expectError           bool
	}{
		{
			name:                  "read all manifests",
			assetDir:              "testdata",
			expectedManifestCount: 5,
		},
		{
			name:        "handle missing dir",
			assetDir:    "foo",
			expectError: true,
		},
		{
			name: "read only 00_ prefixed files",
			options: CreateOptions{
				Filters: []assets.FileInfoPredicate{
					func(info os.FileInfo) bool {
						return strings.HasPrefix(info.Name(), "00")
					},
				},
			},
			assetDir:              "testdata",
			expectedManifestCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := load(tc.assetDir, tc.options)
			if tc.expectError && err == nil {
				t.Errorf("expected error, got no error")
				return
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if len(result) != tc.expectedManifestCount {
				t.Errorf("expected %d manifests loaded, got %d", tc.expectedManifestCount, len(result))
				return
			}
		})
	}
}
