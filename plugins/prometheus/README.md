# shellcn-contrib/plugins/prometheus

ShellCN-maintained external plugin for Prometheus.

It provides PromQL query, targets, alerts, rules, labels, metric metadata,
series, status, live overview metrics, and gated admin operations. It lives in
contrib because many teams already operate Prometheus through their observability
stack, while ShellCN still keeps the plugin available through the Marketplace.

## Build

```sh
make build PLUGIN=prometheus
```

The binary is written to `dist/shellcn-plugin-prometheus` from the repository
root.
