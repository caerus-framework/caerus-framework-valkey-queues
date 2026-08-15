package chassis

import (
	"fmt"

	cf "github.com/caerus-framework/caerus-framework"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
)

// PeerName is the valkey component Name() to resolve. Empty means the
// default cf_valkey.ComponentName ("valkey").
func PeerName(valkeyName string) string {
	if valkeyName != "" {
		return valkeyName
	}
	return cf_valkey.ComponentName
}

// ResolveValkey looks up the valkey peer by component name. It does not
// require Client() to be non-nil (soft-init / DegradedMode).
func ResolveValkey(fw *cf.CaerusFramework, valkeyName string) (*cf_valkey.CFValkey, error) {
	name := PeerName(valkeyName)
	var vk *cf_valkey.CFValkey
	var ok bool
	if valkeyName == "" {
		vk, ok = cf.Get[*cf_valkey.CFValkey](fw)
	} else {
		vk, ok = cf.GetByName[*cf_valkey.CFValkey](fw, valkeyName)
	}
	if !ok || vk == nil {
		return nil, fmt.Errorf("valkey-queues: valkey component %q is not registered", name)
	}
	return vk, nil
}
