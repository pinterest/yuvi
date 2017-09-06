package com.pinterest.yuvi.tagstore;

/**
 * The match types match all the standard TagVFilter types in OpenTSDB 2.3. More info on these
 * types is at: http://opentsdb.net/docs/build/html/user_guide/query/filters.html
 */
public enum MatchType {
  EXACT,
  WILDCARD, IWILDCARD,
  LITERAL_OR, ILITERAL_OR,
  NOT_LITERAL_OR, NOT_ILITERAL_OR,
  REGEXP
}
