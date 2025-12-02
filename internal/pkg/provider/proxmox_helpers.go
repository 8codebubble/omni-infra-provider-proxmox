package provider

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"

    // proxmox client is used via Provisioner.proxmoxClient
)

// findISOStorageName queries Proxmox for storages on a node and returns
// the first storage that advertises support for "iso" content.
func (p *Provisioner) findISOStorageName(ctx context.Context, node string) (string, error) {
    // Fetch storage list for the node. Use a raw JSON container to be resilient
    // against variability in the returned shape.
    var raw json.RawMessage
    if err := p.proxmoxClient.Get(ctx, fmt.Sprintf("/nodes/%s/storage", node), &raw); err != nil {
        return "", err
    }

    // Try to unmarshal into a slice of generic objects
    var items []map[string]any
    if err := json.Unmarshal(raw, &items); err != nil {
        return "", fmt.Errorf("failed to parse storage list: %w", err)
    }

    for _, item := range items {
        // storage name may be under "storage" or "name"
        var name string
        if s, ok := item["storage"].(string); ok {
            name = s
        } else if s, ok := item["name"].(string); ok {
            name = s
        }

        if name == "" {
            continue
        }

        // content can be array of strings
        if content, ok := item["content"].([]any); ok {
            for _, c := range content {
                if cs, ok := c.(string); ok && cs == "iso" {
                    return name, nil
                }
            }
        }
    }

    return "", fmt.Errorf("no ISO-capable storage found on node %s", node)
}

// storageHasISO checks whether the given storage on a node already contains
// an ISO with the provided name.
func (p *Provisioner) storageHasISO(ctx context.Context, node, storage, isoName string) (bool, error) {
    var raw json.RawMessage
    if err := p.proxmoxClient.Get(ctx, fmt.Sprintf("/nodes/%s/storage/%s/content", node, storage), &raw); err != nil {
        return false, err
    }

    var items []map[string]any
    if err := json.Unmarshal(raw, &items); err != nil {
        return false, fmt.Errorf("failed to parse storage content: %w", err)
    }

    for _, it := range items {
        // volid can be like <storage>:isoName or just isoName
        if v, ok := it["volid"].(string); ok {
            if strings.HasSuffix(v, isoName) || v == isoName {
                return true, nil
            }
        }
        if n, ok := it["name"].(string); ok {
            if n == isoName {
                return true, nil
            }
        }
    }

    return false, nil
}

// startStorageDownload triggers a storage download task for an ISO and returns the task UPID as string.
func (p *Provisioner) startStorageDownload(ctx context.Context, node, storage, isoName, sourceURL string) (string, error) {
    // Prepare parameters according to Proxmox storage download API
    params := map[string]string{
        "content":  "iso",
        "filename": isoName,
        "url":      sourceURL,
    }

    // The go-proxmox client expects a target to unmarshal response into.
    // We'll unmarshal into a generic map and extract the UPID from returned data.
    var resp json.RawMessage
    path := fmt.Sprintf("/nodes/%s/storage/%s/download", node, storage)
    if err := p.proxmoxClient.Post(ctx, path, &resp, params); err != nil {
        return "", err
    }

    // Response may be {"data":"UPID:..."} or {"data":{"upid":"..."}}
    var wrapper map[string]any
    if err := json.Unmarshal(resp, &wrapper); err != nil {
        return "", fmt.Errorf("failed to parse download response: %w", err)
    }

    if d, ok := wrapper["data"]; ok {
        switch v := d.(type) {
        case string:
            return v, nil
        case map[string]any:
            if upid, ok := v["upid"].(string); ok {
                return upid, nil
            }
        }
    }

    return "", fmt.Errorf("unexpected download response: %v", wrapper)
}
