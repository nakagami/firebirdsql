package firebirdsql

import (
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
)

func charsetEncoding(charset string) encoding.Encoding {
	switch charset {
	case "SJIS_0208":
		return japanese.ShiftJIS
	case "EUCJ_0208":
		return japanese.EUCJP
	case "ISO8859_1":
		return charmap.ISO8859_1
	case "ISO8859_2":
		return charmap.ISO8859_2
	case "ISO8859_3":
		return charmap.ISO8859_3
	case "ISO8859_4":
		return charmap.ISO8859_4
	case "ISO8859_5":
		return charmap.ISO8859_5
	case "ISO8859_6":
		return charmap.ISO8859_6
	case "ISO8859_7":
		return charmap.ISO8859_7
	case "ISO8859_8":
		return charmap.ISO8859_8
	case "ISO8859_9":
		return charmap.ISO8859_9
	case "ISO8859_13":
		return charmap.ISO8859_13
	case "KSC_5601":
		return korean.EUCKR
	case "WIN1250":
		return charmap.Windows1250
	case "WIN1251":
		return charmap.Windows1251
	case "WIN1252":
		return charmap.Windows1252
	case "WIN1253":
		return charmap.Windows1253
	case "WIN1254":
		return charmap.Windows1254
	case "BIG_5":
		return traditionalchinese.Big5
	case "GB_2312":
		return simplifiedchinese.HZGB2312
	case "WIN1255":
		return charmap.Windows1255
	case "WIN1256":
		return charmap.Windows1256
	case "WIN1257":
		return charmap.Windows1257
	case "KOI8R":
		return charmap.KOI8R
	case "KOI8U":
		return charmap.KOI8U
	case "WIN1258":
		return charmap.Windows1258
	default:
		return nil
	}
}

func decodeCharset(raw []byte, charset string) (string, bool) {
	enc := charsetEncoding(charset)
	if enc == nil {
		return "", false
	}
	v, _ := enc.NewDecoder().Bytes(raw)
	return string(v), true
}

func encodeCharset(str string, charset string) (string, bool) {
	enc := charsetEncoding(charset)
	if enc == nil {
		return "", false
	}
	v, _ := enc.NewEncoder().String(str)
	return v, true
}
