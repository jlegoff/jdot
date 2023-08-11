package apmconnector

import (
	"regexp"
	"strings"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type SqlParser struct {
	re *regexp.Regexp
}

func NewSqlParser() *SqlParser {
	re, _ := regexp.Compile(`(?i).*?\sfrom[\s\[]+([^\]\s,)(;]*).*`)
	return &SqlParser{re: re}
}

func (sqlParser *SqlParser) ParseDbTableFromSql(sql string) (string, bool) {
	matches := sqlParser.re.FindStringSubmatch(sql)
	count := len(matches)
	if count < 2 {
		return "", false
	}
	return strings.ToLower(matches[1]), true
}

func (sqlParser *SqlParser) ParseDbTableFromSpan(span ptrace.Span) (string, bool) {
	dbTable, dbTablePresent := span.Attributes().Get(DbSqlTableAttributeName)
	if dbTablePresent {
		return dbTable.AsString(), false
	} else {
		if sql, sqlPresent := span.Attributes().Get("db.statement"); sqlPresent {
			if parsedTable, exists := sqlParser.ParseDbTableFromSql(sql.AsString()); exists {
				return parsedTable, true
			}
		}
	}
	return "unknown", false
}
