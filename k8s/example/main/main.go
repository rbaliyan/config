// Command kubeclient-demo wires the example Adapter into a [k8s.Store] and
// performs a single set/get round trip. It is intended as documentation in
// runnable form rather than a production starter.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/rbaliyan/config"
	_ "github.com/rbaliyan/config/codec/json"
	"github.com/rbaliyan/config/k8s"
	kubeclient "github.com/rbaliyan/config/k8s/example"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, _ := os.UserHomeDir()
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("load kubeconfig: %v", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("build clientset: %v", err)
	}

	store := k8s.NewStore(kubeclient.New(cs), k8s.WithK8sNamespace("default"))
	ctx := context.Background()
	if err := store.Connect(ctx); err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() { _ = store.Close(ctx) }()

	val := config.NewValue(map[string]any{"region": "us-east-1", "tier": "prod"})
	if _, err := store.Set(ctx, "demo", "app/settings", val); err != nil {
		log.Fatalf("set: %v", err)
	}

	got, err := store.Get(ctx, "demo", "app/settings")
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	var out map[string]any
	if err := got.Unmarshal(ctx, &out); err != nil {
		log.Fatalf("unmarshal: %v", err)
	}
	fmt.Printf("settings: %+v\n", out)
}
