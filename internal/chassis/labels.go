package chassis

// CopyLabels returns a shallow copy of a label map so scrapers cannot mutate
// the component's stored labels.
func CopyLabels(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
