// Generated from /home/florian/Workspace/stream-thoughts/GitHub/kafka-connect-file-pulse/connect-file-pulse-expression/src/main/antlr4/io/streamthoughts/kafka/connect/filepulse/expression/parser/antlr4/ScELLexer.g4 by ANTLR 4.8
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ScELLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		Literal=1, StringLiteral=2, BooleanLiteral=3, NullLiteral=4, IntegerLiteral=5, 
		DOT=6, LPAREN=7, RPAREN=8, COMMA=9, LBRACE=10, RBRACE=11, UNDER_SCORE=12, 
		NUMBER=13, FLOAT=14, WS=15, QUOTE=16, LineStrText=17, LineStrEscapedChar=18, 
		LineSubstExprStart=19, PropertyExprStart=20, Letter=21, LetterOrDigit=22, 
		Identifier=23, LineSubstExprEnd=24, Substr_WS=25;
	public static final int
		PropertyExpression=1, SubstitutionExpression=2;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE", "PropertyExpression", "SubstitutionExpression"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"Literal", "StringLiteral", "EscapeSeq", "EscapedIdentifier", "EscapeSequence", 
			"BooleanLiteral", "NullLiteral", "T", "F", "IntegerLiteral", "DecimalNumeral", 
			"Digits", "Digit", "NonZeroDigit", "DigitsAndUnderscores", "DigitOrUnderscore", 
			"HexDigit", "Underscores", "UniCharacterLiteral", "DOT", "LPAREN", "RPAREN", 
			"COMMA", "LBRACE", "RBRACE", "UNDER_SCORE", "NUMBER", "FLOAT", "WS", 
			"QUOTE", "LineStrText", "LineStrEscapedChar", "LineSubstExprStart", "PropertyExprStart", 
			"Letter", "LetterOrDigit", "Identifier", "Prop_DOT", "LineSubstExprEnd", 
			"Substr_LinePropertyExprStart", "Substr_Literal", "Substr_Identifier", 
			"Substr_COMMA", "Substr_DOT", "Substr_LPAREN", "Substr_RPAREN", "Substr_WS"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, null, null, null, "'null'", null, "'.'", "'('", "')'", "','", "'{'", 
			"'}'", "'_'", null, null, null, "'''", null, null, "'{{'", "'$'", null, 
			null, null, "'}}'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "Literal", "StringLiteral", "BooleanLiteral", "NullLiteral", "IntegerLiteral", 
			"DOT", "LPAREN", "RPAREN", "COMMA", "LBRACE", "RBRACE", "UNDER_SCORE", 
			"NUMBER", "FLOAT", "WS", "QUOTE", "LineStrText", "LineStrEscapedChar", 
			"LineSubstExprStart", "PropertyExprStart", "Letter", "LetterOrDigit", 
			"Identifier", "LineSubstExprEnd", "Substr_WS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public ScELLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "ScELLexer.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\33\u0149\b\1\b\1"+
		"\b\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4"+
		"\n\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t"+
		"\21\4\22\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t"+
		"\30\4\31\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t"+
		"\37\4 \t \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4"+
		"*\t*\4+\t+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\3\2\3\2\3\2\3\2\5\2h\n\2"+
		"\3\3\3\3\3\3\6\3m\n\3\r\3\16\3n\3\3\3\3\3\4\3\4\5\4u\n\4\3\5\3\5\3\5\3"+
		"\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u0088\n\7\3"+
		"\b\3\b\3\b\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\3\f\5\f\u0098\n\f"+
		"\3\f\3\f\3\f\5\f\u009d\n\f\5\f\u009f\n\f\3\r\3\r\5\r\u00a3\n\r\3\r\5\r"+
		"\u00a6\n\r\3\16\3\16\5\16\u00aa\n\16\3\17\3\17\3\20\6\20\u00af\n\20\r"+
		"\20\16\20\u00b0\3\21\3\21\5\21\u00b5\n\21\3\22\3\22\3\23\6\23\u00ba\n"+
		"\23\r\23\16\23\u00bb\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\26"+
		"\3\26\3\27\3\27\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\34\6\34\u00d4"+
		"\n\34\r\34\16\34\u00d5\3\34\3\34\7\34\u00da\n\34\f\34\16\34\u00dd\13\34"+
		"\3\35\7\35\u00e0\n\35\f\35\16\35\u00e3\13\35\3\35\3\35\6\35\u00e7\n\35"+
		"\r\35\16\35\u00e8\3\35\3\35\6\35\u00ed\n\35\r\35\16\35\u00ee\7\35\u00f1"+
		"\n\35\f\35\16\35\u00f4\13\35\3\36\6\36\u00f7\n\36\r\36\16\36\u00f8\3\36"+
		"\3\36\3\37\3\37\3\37\3\37\3 \6 \u0102\n \r \16 \u0103\3 \5 \u0107\n \3"+
		"!\3!\3!\5!\u010c\n!\3\"\3\"\3\"\3\"\3\"\3#\3#\3#\3#\3$\3$\3%\3%\3&\3&"+
		"\6&\u011d\n&\r&\16&\u011e\3\'\3\'\3\'\3\'\3(\3(\3(\3(\3(\3)\3)\3)\3)\3"+
		"*\3*\3*\3*\3+\3+\3+\3+\3,\3,\3,\3,\3-\3-\3-\3-\3.\3.\3.\3.\3/\3/\3/\3"+
		"/\3\60\3\60\3\60\3\60\3n\2\61\5\3\7\4\t\2\13\2\r\2\17\5\21\6\23\2\25\2"+
		"\27\7\31\2\33\2\35\2\37\2!\2#\2%\2\'\2)\2+\b-\t/\n\61\13\63\f\65\r\67"+
		"\169\17;\20=\21?\22A\23C\24E\25G\26I\27K\30M\31O\2Q\32S\2U\2W\2Y\2[\2"+
		"]\2_\2a\33\5\2\3\4\r\n\2$$&&))^^ddppttvv\n\2$$))^^ddhhppttvv\4\2VVvv\4"+
		"\2HHhh\3\2\63;\5\2\62;CHch\4\2GGgg\5\2\13\f\16\17\"\"\5\2&&^^}}\5\2C\\"+
		"aac|\7\2//\62;C\\aac|\2\u0153\2\5\3\2\2\2\2\7\3\2\2\2\2\17\3\2\2\2\2\21"+
		"\3\2\2\2\2\27\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2"+
		"\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2"+
		"\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\3I\3\2\2\2\3K"+
		"\3\2\2\2\3M\3\2\2\2\3O\3\2\2\2\4Q\3\2\2\2\4S\3\2\2\2\4U\3\2\2\2\4W\3\2"+
		"\2\2\4Y\3\2\2\2\4[\3\2\2\2\4]\3\2\2\2\4_\3\2\2\2\4a\3\2\2\2\5g\3\2\2\2"+
		"\7i\3\2\2\2\tt\3\2\2\2\13v\3\2\2\2\ry\3\2\2\2\17\u0087\3\2\2\2\21\u0089"+
		"\3\2\2\2\23\u008e\3\2\2\2\25\u0090\3\2\2\2\27\u0092\3\2\2\2\31\u009e\3"+
		"\2\2\2\33\u00a0\3\2\2\2\35\u00a9\3\2\2\2\37\u00ab\3\2\2\2!\u00ae\3\2\2"+
		"\2#\u00b4\3\2\2\2%\u00b6\3\2\2\2\'\u00b9\3\2\2\2)\u00bd\3\2\2\2+\u00c4"+
		"\3\2\2\2-\u00c6\3\2\2\2/\u00c8\3\2\2\2\61\u00ca\3\2\2\2\63\u00cc\3\2\2"+
		"\2\65\u00ce\3\2\2\2\67\u00d0\3\2\2\29\u00d3\3\2\2\2;\u00e1\3\2\2\2=\u00f6"+
		"\3\2\2\2?\u00fc\3\2\2\2A\u0106\3\2\2\2C\u010b\3\2\2\2E\u010d\3\2\2\2G"+
		"\u0112\3\2\2\2I\u0116\3\2\2\2K\u0118\3\2\2\2M\u011a\3\2\2\2O\u0120\3\2"+
		"\2\2Q\u0124\3\2\2\2S\u0129\3\2\2\2U\u012d\3\2\2\2W\u0131\3\2\2\2Y\u0135"+
		"\3\2\2\2[\u0139\3\2\2\2]\u013d\3\2\2\2_\u0141\3\2\2\2a\u0145\3\2\2\2c"+
		"h\5\7\3\2dh\5\21\b\2eh\5\27\13\2fh\5\17\7\2gc\3\2\2\2gd\3\2\2\2ge\3\2"+
		"\2\2gf\3\2\2\2h\6\3\2\2\2il\7)\2\2jm\5\t\4\2km\13\2\2\2lj\3\2\2\2lk\3"+
		"\2\2\2mn\3\2\2\2no\3\2\2\2nl\3\2\2\2op\3\2\2\2pq\7)\2\2q\b\3\2\2\2ru\5"+
		")\24\2su\5\13\5\2tr\3\2\2\2ts\3\2\2\2u\n\3\2\2\2vw\7^\2\2wx\t\2\2\2x\f"+
		"\3\2\2\2yz\7^\2\2z{\t\3\2\2{\16\3\2\2\2|}\5\23\t\2}~\7t\2\2~\177\7w\2"+
		"\2\177\u0080\7g\2\2\u0080\u0088\3\2\2\2\u0081\u0082\5\25\n\2\u0082\u0083"+
		"\7c\2\2\u0083\u0084\7n\2\2\u0084\u0085\7u\2\2\u0085\u0086\7g\2\2\u0086"+
		"\u0088\3\2\2\2\u0087|\3\2\2\2\u0087\u0081\3\2\2\2\u0088\20\3\2\2\2\u0089"+
		"\u008a\7p\2\2\u008a\u008b\7w\2\2\u008b\u008c\7n\2\2\u008c\u008d\7n\2\2"+
		"\u008d\22\3\2\2\2\u008e\u008f\t\4\2\2\u008f\24\3\2\2\2\u0090\u0091\t\5"+
		"\2\2\u0091\26\3\2\2\2\u0092\u0093\5\31\f\2\u0093\30\3\2\2\2\u0094\u009f"+
		"\7\62\2\2\u0095\u009c\5\37\17\2\u0096\u0098\5\33\r\2\u0097\u0096\3\2\2"+
		"\2\u0097\u0098\3\2\2\2\u0098\u009d\3\2\2\2\u0099\u009a\5\'\23\2\u009a"+
		"\u009b\5\33\r\2\u009b\u009d\3\2\2\2\u009c\u0097\3\2\2\2\u009c\u0099\3"+
		"\2\2\2\u009d\u009f\3\2\2\2\u009e\u0094\3\2\2\2\u009e\u0095\3\2\2\2\u009f"+
		"\32\3\2\2\2\u00a0\u00a5\5\35\16\2\u00a1\u00a3\5!\20\2\u00a2\u00a1\3\2"+
		"\2\2\u00a2\u00a3\3\2\2\2\u00a3\u00a4\3\2\2\2\u00a4\u00a6\5\35\16\2\u00a5"+
		"\u00a2\3\2\2\2\u00a5\u00a6\3\2\2\2\u00a6\34\3\2\2\2\u00a7\u00aa\7\62\2"+
		"\2\u00a8\u00aa\5\37\17\2\u00a9\u00a7\3\2\2\2\u00a9\u00a8\3\2\2\2\u00aa"+
		"\36\3\2\2\2\u00ab\u00ac\t\6\2\2\u00ac \3\2\2\2\u00ad\u00af\5#\21\2\u00ae"+
		"\u00ad\3\2\2\2\u00af\u00b0\3\2\2\2\u00b0\u00ae\3\2\2\2\u00b0\u00b1\3\2"+
		"\2\2\u00b1\"\3\2\2\2\u00b2\u00b5\5\35\16\2\u00b3\u00b5\5\67\33\2\u00b4"+
		"\u00b2\3\2\2\2\u00b4\u00b3\3\2\2\2\u00b5$\3\2\2\2\u00b6\u00b7\t\7\2\2"+
		"\u00b7&\3\2\2\2\u00b8\u00ba\5\67\33\2\u00b9\u00b8\3\2\2\2\u00ba\u00bb"+
		"\3\2\2\2\u00bb\u00b9\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc(\3\2\2\2\u00bd"+
		"\u00be\7^\2\2\u00be\u00bf\7w\2\2\u00bf\u00c0\5%\22\2\u00c0\u00c1\5%\22"+
		"\2\u00c1\u00c2\5%\22\2\u00c2\u00c3\5%\22\2\u00c3*\3\2\2\2\u00c4\u00c5"+
		"\7\60\2\2\u00c5,\3\2\2\2\u00c6\u00c7\7*\2\2\u00c7.\3\2\2\2\u00c8\u00c9"+
		"\7+\2\2\u00c9\60\3\2\2\2\u00ca\u00cb\7.\2\2\u00cb\62\3\2\2\2\u00cc\u00cd"+
		"\7}\2\2\u00cd\64\3\2\2\2\u00ce\u00cf\7\177\2\2\u00cf\66\3\2\2\2\u00d0"+
		"\u00d1\7a\2\2\u00d18\3\2\2\2\u00d2\u00d4\4\62;\2\u00d3\u00d2\3\2\2\2\u00d4"+
		"\u00d5\3\2\2\2\u00d5\u00d3\3\2\2\2\u00d5\u00d6\3\2\2\2\u00d6\u00db\3\2"+
		"\2\2\u00d7\u00d8\t\b\2\2\u00d8\u00da\59\34\2\u00d9\u00d7\3\2\2\2\u00da"+
		"\u00dd\3\2\2\2\u00db\u00d9\3\2\2\2\u00db\u00dc\3\2\2\2\u00dc:\3\2\2\2"+
		"\u00dd\u00db\3\2\2\2\u00de\u00e0\4\62;\2\u00df\u00de\3\2\2\2\u00e0\u00e3"+
		"\3\2\2\2\u00e1\u00df\3\2\2\2\u00e1\u00e2\3\2\2\2\u00e2\u00e4\3\2\2\2\u00e3"+
		"\u00e1\3\2\2\2\u00e4\u00e6\7\60\2\2\u00e5\u00e7\4\62;\2\u00e6\u00e5\3"+
		"\2\2\2\u00e7\u00e8\3\2\2\2\u00e8\u00e6\3\2\2\2\u00e8\u00e9\3\2\2\2\u00e9"+
		"\u00f2\3\2\2\2\u00ea\u00ec\t\b\2\2\u00eb\u00ed\4\62;\2\u00ec\u00eb\3\2"+
		"\2\2\u00ed\u00ee\3\2\2\2\u00ee\u00ec\3\2\2\2\u00ee\u00ef\3\2\2\2\u00ef"+
		"\u00f1\3\2\2\2\u00f0\u00ea\3\2\2\2\u00f1\u00f4\3\2\2\2\u00f2\u00f0\3\2"+
		"\2\2\u00f2\u00f3\3\2\2\2\u00f3<\3\2\2\2\u00f4\u00f2\3\2\2\2\u00f5\u00f7"+
		"\t\t\2\2\u00f6\u00f5\3\2\2\2\u00f7\u00f8\3\2\2\2\u00f8\u00f6\3\2\2\2\u00f8"+
		"\u00f9\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa\u00fb\b\36\2\2\u00fb>\3\2\2\2"+
		"\u00fc\u00fd\7)\2\2\u00fd\u00fe\3\2\2\2\u00fe\u00ff\b\37\2\2\u00ff@\3"+
		"\2\2\2\u0100\u0102\n\n\2\2\u0101\u0100\3\2\2\2\u0102\u0103\3\2\2\2\u0103"+
		"\u0101\3\2\2\2\u0103\u0104\3\2\2\2\u0104\u0107\3\2\2\2\u0105\u0107\5\63"+
		"\31\2\u0106\u0101\3\2\2\2\u0106\u0105\3\2\2\2\u0107B\3\2\2\2\u0108\u0109"+
		"\7^\2\2\u0109\u010c\13\2\2\2\u010a\u010c\5)\24\2\u010b\u0108\3\2\2\2\u010b"+
		"\u010a\3\2\2\2\u010cD\3\2\2\2\u010d\u010e\7}\2\2\u010e\u010f\7}\2\2\u010f"+
		"\u0110\3\2\2\2\u0110\u0111\b\"\3\2\u0111F\3\2\2\2\u0112\u0113\7&\2\2\u0113"+
		"\u0114\3\2\2\2\u0114\u0115\b#\4\2\u0115H\3\2\2\2\u0116\u0117\t\13\2\2"+
		"\u0117J\3\2\2\2\u0118\u0119\t\f\2\2\u0119L\3\2\2\2\u011a\u011c\5I$\2\u011b"+
		"\u011d\5K%\2\u011c\u011b\3\2\2\2\u011d\u011e\3\2\2\2\u011e\u011c\3\2\2"+
		"\2\u011e\u011f\3\2\2\2\u011fN\3\2\2\2\u0120\u0121\5+\25\2\u0121\u0122"+
		"\3\2\2\2\u0122\u0123\b\'\5\2\u0123P\3\2\2\2\u0124\u0125\7\177\2\2\u0125"+
		"\u0126\7\177\2\2\u0126\u0127\3\2\2\2\u0127\u0128\b(\6\2\u0128R\3\2\2\2"+
		"\u0129\u012a\5G#\2\u012a\u012b\3\2\2\2\u012b\u012c\b)\7\2\u012cT\3\2\2"+
		"\2\u012d\u012e\5\5\2\2\u012e\u012f\3\2\2\2\u012f\u0130\b*\b\2\u0130V\3"+
		"\2\2\2\u0131\u0132\5M&\2\u0132\u0133\3\2\2\2\u0133\u0134\b+\t\2\u0134"+
		"X\3\2\2\2\u0135\u0136\5\61\30\2\u0136\u0137\3\2\2\2\u0137\u0138\b,\n\2"+
		"\u0138Z\3\2\2\2\u0139\u013a\5+\25\2\u013a\u013b\3\2\2\2\u013b\u013c\b"+
		"-\5\2\u013c\\\3\2\2\2\u013d\u013e\5-\26\2\u013e\u013f\3\2\2\2\u013f\u0140"+
		"\b.\13\2\u0140^\3\2\2\2\u0141\u0142\5/\27\2\u0142\u0143\3\2\2\2\u0143"+
		"\u0144\b/\f\2\u0144`\3\2\2\2\u0145\u0146\5=\36\2\u0146\u0147\3\2\2\2\u0147"+
		"\u0148\b\60\2\2\u0148b\3\2\2\2\36\2\3\4glnt\u0087\u0097\u009c\u009e\u00a2"+
		"\u00a5\u00a9\u00b0\u00b4\u00bb\u00d5\u00db\u00e1\u00e8\u00ee\u00f2\u00f8"+
		"\u0103\u0106\u010b\u011e\r\b\2\2\7\4\2\7\3\2\t\b\2\6\2\2\t\26\2\t\3\2"+
		"\t\31\2\t\13\2\t\t\2\t\n\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}