package bome

import "strings"

func escaped(value string) string {
	replace := map[string]string{"\\": "\\\\", "'": `\'`, "\\0": "\\\\0", "\n": "\\n", "\r": "\\r", `"`: `\"`, "\x1a": "\\Z"}
	for b, a := range replace {
		value = strings.Replace(value, b, a, -1)
	}
	return value
}

func normalizedJsonPath(jp string) string {
	jp = strings.Replace(jp, "/", ".", -1)
	if strings.HasPrefix(jp, "$.") {
		return jp
	}
	if strings.HasPrefix(jp, ".") {
		return "$" + jp
	}
	return "$." + jp
}
