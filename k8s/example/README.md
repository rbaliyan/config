# Kubernetes adapter example

This module is a reference implementation of the
[`k8s.Client`](../client.go) interface defined by `github.com/rbaliyan/config/k8s`.
It is kept in a separate Go module so the main `config` module never depends on
`k8s.io/*` packages — config and the k8s store can be released on a single
cadence, while this adapter follows the kubernetes client release cycle.

## What the adapter provides

| `k8s.Client` method | Implementation |
|---|---|
| `Get`     | `CoreV1().{ConfigMaps,Secrets}.Get` |
| `Upsert`  | Create-or-Update on conflict (`IsAlreadyExists` → Update) |
| `Watch`   | `CoreV1().{ConfigMaps,Secrets}.Watch` merged into one channel |
| `Health`  | `CoreV1().Namespaces.List(Limit: 1)` |

There is **no informer cache** — reads go straight to the API server and the
config Manager handles repeat-read caching. This mirrors how the postgres and
mongodb stores behave.

## Wiring it up

```go
import (
    "context"

    "github.com/rbaliyan/config/k8s"
    kubeclient "github.com/rbaliyan/config/k8s/example"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
)

ctx := context.Background()

cfg, _ := rest.InClusterConfig()           // or clientcmd.BuildConfigFromFlags(...)
cs, _  := kubernetes.NewForConfig(cfg)

store := k8s.NewStore(kubeclient.New(cs),
    k8s.WithK8sNamespace("config-system"), // pin to one k8s namespace
    k8s.WithSecretKeyPrefix("secret/"),    // route secret/* keys to k8s Secrets
)

if err := store.Connect(ctx); err != nil { ... }
defer store.Close(ctx)
```

See [`main/main.go`](./main/main.go) for a runnable end-to-end demo.

## Notes for production use

- **Reconnect on watch loss.** The adapter does not automatically resume a
  dropped watch. Wrap the upstream `watch.Interface` with
  [`watch.NewRetryWatcher`](https://pkg.go.dev/k8s.io/client-go/tools/watch#NewRetryWatcher)
  if you need that behavior; the rest of the code remains unchanged.
- **RBAC.** The service account needs `get`, `list`, `watch`, `create`, `update`
  on `configmaps` and `secrets` in the target namespace, plus `list` on
  `namespaces` for the health check.
- **Annotations.** The store records the codec for each key in an annotation
  (`config.rbaliyan.dev/codec-<key>`). The adapter must round-trip annotations
  exactly; this implementation does so via the standard ConfigMap/Secret
  metadata.
