// Generates Rust bundle goldens using the unmodified frozen Go BuildManifest body.
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

func main() {
	cases := []Skill{
		{ID: "skill-1", Source: SourceWorkspace, Name: "deploy", Content: "main", Files: []File{{Path: "b.md", Content: "b"}, {Path: "a.md", Content: "a"}}},
		{ID: "skill-1", Source: SourceWorkspace, Name: "triển khai", Description: "Dữ liệu\n2:x", Content: "# Nội dung 🦀\n", Files: []File{{Path: "空.md", Content: "a\nb:c\x00"}, {Path: "empty", Content: ""}}},
		{ID: "empty", Source: SourceWorkspace, Name: "empty"},
	}
	for _, root := range os.Args[1:] {
		dirs, err := os.ReadDir(root)
		if err != nil {
			panic(err)
		}
		for _, dir := range dirs {
			if !dir.IsDir() {
				continue
			}
			path := filepath.Join(root, dir.Name())
			main, err := os.ReadFile(filepath.Join(path, "SKILL.md"))
			if err != nil {
				panic(err)
			}
			sk := Skill{ID: "builtin:" + dir.Name(), Source: SourceBuiltin, Name: dir.Name(), Content: string(main)}
			err = filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.IsDir() {
					return nil
				}
				rel, err := filepath.Rel(path, p)
				if err != nil {
					return err
				}
				if rel == "SKILL.md" {
					return nil
				}
				bytes, err := os.ReadFile(p)
				if err != nil {
					return err
				}
				sk.Files = append(sk.Files, File{Path: filepath.ToSlash(rel), Content: string(bytes)})
				return nil
			})
			if err != nil {
				panic(err)
			}
			sort.Slice(sk.Files, func(i, j int) bool { return sk.Files[i].Path < sk.Files[j].Path })
			cases = append(cases, sk)
		}
	}
	out := []any{}
	for _, sk := range cases {
		out = append(out, map[string]any{"skill": sk, "manifest": BuildManifest(sk)})
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(out); err != nil {
		panic(err)
	}
}
