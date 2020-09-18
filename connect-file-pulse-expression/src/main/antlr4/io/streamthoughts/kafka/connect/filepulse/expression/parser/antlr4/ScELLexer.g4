lexer grammar ScELLexer;

Literal
	:	StringLiteral
	|	NullLiteral
	|   IntegerLiteral
	|   BooleanLiteral
	;

StringLiteral
    : '\'' (EscapeSeq | .)*? '\''
    ;

fragment EscapeSeq
    : UniCharacterLiteral
    | EscapedIdentifier
    ;

fragment EscapedIdentifier
    : '\\' ('t' | 'b' | 'r' | 'n' | '\'' | '"' | '\\' | '$')
    ;

fragment
EscapeSequence
	:	'\\' [btnfr"'\\]
	;

// Boolean Literals
BooleanLiteral
	:   T'rue'
	|	F'alse'
	;

// Null Literals

NullLiteral
	:	'null'
	;

fragment
T   : ('t' | 'T' );

fragment
F   : ('f' | 'F' );

// Integer Literal

IntegerLiteral
    : DecimalNumeral
    ;

fragment
DecimalNumeral
	:	'0'
	|	NonZeroDigit (Digits? | Underscores Digits)
	;

fragment
Digits
	:	Digit (DigitsAndUnderscores? Digit)?
	;

fragment
Digit
	:	'0'
	|	NonZeroDigit
	;

fragment
NonZeroDigit
	:	[1-9]
	;

fragment
DigitsAndUnderscores
	:	DigitOrUnderscore+
	;

fragment
DigitOrUnderscore
	:	Digit
	|	UNDER_SCORE
	;

fragment HexDigit
    : [0-9a-fA-F]
    ;

fragment
Underscores
	:	UNDER_SCORE +
	;

fragment UniCharacterLiteral
    : '\\' 'u' HexDigit HexDigit HexDigit HexDigit
    ;


DOT : '.';
LPAREN  : '(';
RPAREN  : ')';
COMMA   : ',' ;
LBRACE  : '{';
RBRACE  : '}';
UNDER_SCORE : '_';

NUMBER
   : ('0' .. '9') + (('e' | 'E') NUMBER)*
   ;
FLOAT
   : ('0' .. '9')* '.' ('0' .. '9') + (('e' | 'E') ('0' .. '9') +)*
   ;

WS  :  [ \t\r\n\u000C]+ -> skip
    ;

QUOTE:	'\'' -> skip;

LineStrText
    : ~('\\' | '{' | '$')+ | LBRACE
    ;

LineStrEscapedChar
    : '\\' .
    | UniCharacterLiteral
    ;

LineSubstExprStart
    : '{{' -> pushMode(SubstitutionExpression)
    ;

PropertyExprStart
    : '$' -> pushMode(PropertyExpression)
    ;

mode PropertyExpression;

Letter
   : [a-zA-Z_]
   ;

LetterOrDigit
   : [a-zA-Z0-9_-]
   ;

Identifier
    : Letter LetterOrDigit+
    ;

Prop_DOT: DOT -> type(DOT);

mode SubstitutionExpression;

LineSubstExprEnd
    : '}}' -> popMode
    ;

Substr_LinePropertyExprStart: PropertyExprStart -> type(PropertyExprStart);
Substr_Literal: Literal -> type(Literal);
Substr_Identifier: Identifier -> type(Identifier);
Substr_COMMA: COMMA -> type(COMMA);
Substr_DOT: DOT -> type(DOT);
Substr_LPAREN: LPAREN -> type(LPAREN);
Substr_RPAREN: RPAREN-> type(RPAREN);
Substr_WS: WS -> skip;