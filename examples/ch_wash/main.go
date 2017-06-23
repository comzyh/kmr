package main

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/naturali/kmr/executor"
	kmrpb "github.com/naturali/kmr/pb"
)

var (
	WASHER_SPLIT_PATTERN = `,|\.|!|\?|，|。|！|？|:|：|;|；|「|」|．|\t|：…｛｝`
	WASHER_IGNORE_PUNCTS = " 　'\"《》‘’“”・-_<>〃〈〉()（）……@、【】[]*-、『』~"

	ignorePuncts map[rune]bool
)

func is_alpha_or_number(r rune) bool {
	return 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || unicode.IsDigit(r)
}

func is_chinese(r rune) bool {
	return r >= '\u4e00' && r <= '\u9fa5'
}

func is_ignore_puncts(r rune) bool {
	if _, ok := ignorePuncts[r]; ok {
		return true
	}
	return false
}
func remove_illegal_pattern(line string) string {
	if strings.HasPrefix(line, "<docno>") || strings.HasSuffix(line, "<url>") {
		return ""
	}
	line = strings.Replace(strings.Replace(line, "</a>", "", -1), "<a>", "", -1)
	re, _ := regexp.Compile(`^https?://.*[\r\n]*`)
	line = re.ReplaceAllString(line, "")
	return line
}

func process_single_sentence(line string) []string {
	out := make([]string, 0)
	e_word := ""
	for _, r := range line {
		if is_ignore_puncts(r) {
			if len(e_word) > 0 {
				out = append(out, e_word)
				e_word = ""
			}
		} else if is_chinese(r) {
			if len(e_word) > 0 {
				out = append(out, e_word)
				e_word = ""
			}
			out = append(out, string(r))
		} else if is_alpha_or_number(r) {
			e_word += string(r)
		} else {
			return nil
		}
	}
	if len(e_word) > 0 {
		out = append(out, e_word)
	}
	return out
}

func Map(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV {
	re, _ := regexp.Compile(WASHER_SPLIT_PATTERN)
	out := make(chan *kmrpb.KV, 1024)
	go func() {
		for kv := range kvs {
			sentence := remove_illegal_pattern(strings.Trim(string(kv.Value), "\n"))
			for _, st := range re.Split(sentence, -1) {
				procceed := strings.Join(process_single_sentence(st), " ")
				if len(procceed) > 0 {
					out <- &kmrpb.KV{Key: []byte(procceed), Value: []byte{1}}
				}
			}
		}
		close(out)
	}()
	return out
}

func Reduce(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV {
	// Deduplicate
	out := make(chan *kmrpb.KV, 1024)
	go func() {
		word := ""
		count := 0
		for kv := range kvs {
			if count == 0 {
				word = string(kv.Key)
			}
			count += 1
		}
		out <- &kmrpb.KV{Key: []byte(word), Value: []byte{1}}
		close(out)
	}()
	return out
}

func main() {
	ignorePuncts = make(map[rune]bool)
	for _, r := range WASHER_IGNORE_PUNCTS {
		ignorePuncts[r] = true
	}

	cw := &executor.ComputeWrap{}
	cw.BindMapper(Map)
	cw.BindReducer(Reduce)
	cw.Run()
}
