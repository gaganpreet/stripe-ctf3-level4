package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"stripe-ctf.com/sqlcluster/log"
    "crypto/sha1"
)

func EnsureAbsent(path string) {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
}

func FmtOutput(out []byte) string {
	o := string(out)
	if strings.ContainsAny(o, "\n") {
		return fmt.Sprintf(`"""
%s"""`, o)
	} else {
		return fmt.Sprintf("%#v", o)
	}
}

func JSONEncode(s interface{}) *bytes.Buffer {
	var b bytes.Buffer
	json.NewEncoder(&b).Encode(s)
	return &b
}

func JSONDecode(body io.Reader, s interface{}) error {
	return json.NewDecoder(body).Decode(s)
}

func Exists(path string) (bool) {
    _, err := os.Stat(path)
    if err == nil { return true}
    if os.IsNotExist(err) { return false }
    return false
}

func Sha1(s string) (string) {
    h := sha1.New()
    h.Write([]byte(s))
    bs := h.Sum(nil)
    res := fmt.Sprintf("%x", bs)
    return res
}

func Compress(s string, flag bool) (string) {
    replacements := make(map[string]string)

    replacements["INSERT"] = "!"
    replacements["CREATE"] = "@"
    replacements["UPDATE"] = "#"
    replacements["SELECT"] = "$"
    replacements["INTO"] = "%"
    replacements["friendCount"] = "^"
    replacements["requestCount"] = "&"
    replacements["favoriteWord"] = "`"
    replacements["WHERE"] = "~"
    replacements["SET"] = "{"
    replacements["name"] = "}"
    replacements["FROM"] = "_"
    replacements["ctf3"] = "|"
    replacements["gdb"] = "["
    replacements["christian"] = "]"
    replacements["carl"] = ":"
    replacements["andy"] = "?"
    replacements["siddarth"] = "\\"

    result := s
    for k, v := range replacements {
        if flag == true {
            result = strings.Replace(result, k, v, -1)
        } else {
            result = strings.Replace(result, v, k, -1)
        }
    }

    // log.Printf("Compressed: ", result)
    return result
}
