package firebirdsql

import (
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
)

var charsetEncodings = map[string]encoding.Encoding{
	"SJIS_0208":  japanese.ShiftJIS,
	"EUCJ_0208":  japanese.EUCJP,
	"ISO8859_1":  charmap.ISO8859_1,
	"ISO8859_2":  charmap.ISO8859_2,
	"ISO8859_3":  charmap.ISO8859_3,
	"ISO8859_4":  charmap.ISO8859_4,
	"ISO8859_5":  charmap.ISO8859_5,
	"ISO8859_6":  charmap.ISO8859_6,
	"ISO8859_7":  charmap.ISO8859_7,
	"ISO8859_8":  charmap.ISO8859_8,
	"ISO8859_9":  charmap.ISO8859_9,
	"ISO8859_13": charmap.ISO8859_13,
	"KSC_5601":   korean.EUCKR,
	"WIN1250":    charmap.Windows1250,
	"WIN1251":    charmap.Windows1251,
	"WIN1252":    charmap.Windows1252,
	"WIN1253":    charmap.Windows1253,
	"WIN1254":    charmap.Windows1254,
	"BIG_5":      traditionalchinese.Big5,
	"GB_2312":    simplifiedchinese.HZGB2312,
	"WIN1255":    charmap.Windows1255,
	"WIN1256":    charmap.Windows1256,
	"WIN1257":    charmap.Windows1257,
	"KOI8R":      charmap.KOI8R,
	"KOI8U":      charmap.KOI8U,
	"WIN1258":    charmap.Windows1258,
}

func decodeCharset(raw []byte, charset string) (string, bool) {
	enc, ok := charsetEncodings[charset]
	if !ok {
		return "", false
	}
	v, _ := enc.NewDecoder().Bytes(raw)
	return string(v), true
}

func encodeCharset(str string, charset string) string {
	enc, ok := charsetEncodings[charset]
	if !ok {
		return str
	}
	v, _ := enc.NewEncoder().String(str)
	return v
}
