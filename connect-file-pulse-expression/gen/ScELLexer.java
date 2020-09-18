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
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\33\u014a\b\1\b\1"+
		"\b\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4"+
		"\n\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t"+
		"\21\4\22\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t"+
		"\30\4\31\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t"+
		"\37\4 \t \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4"+
		"*\t*\4+\t+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\3\2\3\2\3\2\3\2\5\2h\n\2"+
		"\3\3\3\3\3\3\7\3m\n\3\f\3\16\3p\13\3\3\3\3\3\3\4\3\4\5\4v\n\4\3\5\3\5"+
		"\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u0089"+
		"\n\7\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\3\f\5\f\u0099"+
		"\n\f\3\f\3\f\3\f\5\f\u009e\n\f\5\f\u00a0\n\f\3\r\3\r\5\r\u00a4\n\r\3\r"+
		"\5\r\u00a7\n\r\3\16\3\16\5\16\u00ab\n\16\3\17\3\17\3\20\6\20\u00b0\n\20"+
		"\r\20\16\20\u00b1\3\21\3\21\5\21\u00b6\n\21\3\22\3\22\3\23\6\23\u00bb"+
		"\n\23\r\23\16\23\u00bc\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3"+
		"\26\3\26\3\27\3\27\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\34\6\34\u00d5"+
		"\n\34\r\34\16\34\u00d6\3\34\3\34\7\34\u00db\n\34\f\34\16\34\u00de\13\34"+
		"\3\35\7\35\u00e1\n\35\f\35\16\35\u00e4\13\35\3\35\3\35\6\35\u00e8\n\35"+
		"\r\35\16\35\u00e9\3\35\3\35\6\35\u00ee\n\35\r\35\16\35\u00ef\7\35\u00f2"+
		"\n\35\f\35\16\35\u00f5\13\35\3\36\6\36\u00f8\n\36\r\36\16\36\u00f9\3\36"+
		"\3\36\3\37\3\37\3\37\3\37\3 \6 \u0103\n \r \16 \u0104\3 \5 \u0108\n \3"+
		"!\3!\3!\5!\u010d\n!\3\"\3\"\3\"\3\"\3\"\3#\3#\3#\3#\3$\3$\3%\3%\3&\3&"+
		"\6&\u011e\n&\r&\16&\u011f\3\'\3\'\3\'\3\'\3(\3(\3(\3(\3(\3)\3)\3)\3)\3"+
		"*\3*\3*\3*\3+\3+\3+\3+\3,\3,\3,\3,\3-\3-\3-\3-\3.\3.\3.\3.\3/\3/\3/\3"+
		"/\3\60\3\60\3\60\3\60\3n\2\61\5\3\7\4\t\2\13\2\r\2\17\5\21\6\23\2\25\2"+
		"\27\7\31\2\33\2\35\2\37\2!\2#\2%\2\'\2)\2+\b-\t/\n\61\13\63\f\65\r\67"+
		"\169\17;\20=\21?\22A\23C\24E\25G\26I\27K\30M\31O\2Q\32S\2U\2W\2Y\2[\2"+
		"]\2_\2a\33\5\2\3\4\r\n\2$$&&))^^ddppttvv\n\2$$))^^ddhhppttvv\4\2VVvv\4"+
		"\2HHhh\3\2\63;\5\2\62;CHch\4\2GGgg\5\2\13\f\16\17\"\"\5\2&&^^}}\5\2C\\"+
		"aac|\7\2//\62;C\\aac|\2\u0154\2\5\3\2\2\2\2\7\3\2\2\2\2\17\3\2\2\2\2\21"+
		"\3\2\2\2\2\27\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2"+
		"\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2"+
		"\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\3I\3\2\2\2\3K"+
		"\3\2\2\2\3M\3\2\2\2\3O\3\2\2\2\4Q\3\2\2\2\4S\3\2\2\2\4U\3\2\2\2\4W\3\2"+
		"\2\2\4Y\3\2\2\2\4[\3\2\2\2\4]\3\2\2\2\4_\3\2\2\2\4a\3\2\2\2\5g\3\2\2\2"+
		"\7i\3\2\2\2\tu\3\2\2\2\13w\3\2\2\2\rz\3\2\2\2\17\u0088\3\2\2\2\21\u008a"+
		"\3\2\2\2\23\u008f\3\2\2\2\25\u0091\3\2\2\2\27\u0093\3\2\2\2\31\u009f\3"+
		"\2\2\2\33\u00a1\3\2\2\2\35\u00aa\3\2\2\2\37\u00ac\3\2\2\2!\u00af\3\2\2"+
		"\2#\u00b5\3\2\2\2%\u00b7\3\2\2\2\'\u00ba\3\2\2\2)\u00be\3\2\2\2+\u00c5"+
		"\3\2\2\2-\u00c7\3\2\2\2/\u00c9\3\2\2\2\61\u00cb\3\2\2\2\63\u00cd\3\2\2"+
		"\2\65\u00cf\3\2\2\2\67\u00d1\3\2\2\29\u00d4\3\2\2\2;\u00e2\3\2\2\2=\u00f7"+
		"\3\2\2\2?\u00fd\3\2\2\2A\u0107\3\2\2\2C\u010c\3\2\2\2E\u010e\3\2\2\2G"+
		"\u0113\3\2\2\2I\u0117\3\2\2\2K\u0119\3\2\2\2M\u011b\3\2\2\2O\u0121\3\2"+
		"\2\2Q\u0125\3\2\2\2S\u012a\3\2\2\2U\u012e\3\2\2\2W\u0132\3\2\2\2Y\u0136"+
		"\3\2\2\2[\u013a\3\2\2\2]\u013e\3\2\2\2_\u0142\3\2\2\2a\u0146\3\2\2\2c"+
		"h\5\7\3\2dh\5\21\b\2eh\5\27\13\2fh\5\17\7\2gc\3\2\2\2gd\3\2\2\2ge\3\2"+
		"\2\2gf\3\2\2\2h\6\3\2\2\2in\7)\2\2jm\5\t\4\2km\13\2\2\2lj\3\2\2\2lk\3"+
		"\2\2\2mp\3\2\2\2no\3\2\2\2nl\3\2\2\2oq\3\2\2\2pn\3\2\2\2qr\7)\2\2r\b\3"+
		"\2\2\2sv\5)\24\2tv\5\13\5\2us\3\2\2\2ut\3\2\2\2v\n\3\2\2\2wx\7^\2\2xy"+
		"\t\2\2\2y\f\3\2\2\2z{\7^\2\2{|\t\3\2\2|\16\3\2\2\2}~\5\23\t\2~\177\7t"+
		"\2\2\177\u0080\7w\2\2\u0080\u0081\7g\2\2\u0081\u0089\3\2\2\2\u0082\u0083"+
		"\5\25\n\2\u0083\u0084\7c\2\2\u0084\u0085\7n\2\2\u0085\u0086\7u\2\2\u0086"+
		"\u0087\7g\2\2\u0087\u0089\3\2\2\2\u0088}\3\2\2\2\u0088\u0082\3\2\2\2\u0089"+
		"\20\3\2\2\2\u008a\u008b\7p\2\2\u008b\u008c\7w\2\2\u008c\u008d\7n\2\2\u008d"+
		"\u008e\7n\2\2\u008e\22\3\2\2\2\u008f\u0090\t\4\2\2\u0090\24\3\2\2\2\u0091"+
		"\u0092\t\5\2\2\u0092\26\3\2\2\2\u0093\u0094\5\31\f\2\u0094\30\3\2\2\2"+
		"\u0095\u00a0\7\62\2\2\u0096\u009d\5\37\17\2\u0097\u0099\5\33\r\2\u0098"+
		"\u0097\3\2\2\2\u0098\u0099\3\2\2\2\u0099\u009e\3\2\2\2\u009a\u009b\5\'"+
		"\23\2\u009b\u009c\5\33\r\2\u009c\u009e\3\2\2\2\u009d\u0098\3\2\2\2\u009d"+
		"\u009a\3\2\2\2\u009e\u00a0\3\2\2\2\u009f\u0095\3\2\2\2\u009f\u0096\3\2"+
		"\2\2\u00a0\32\3\2\2\2\u00a1\u00a6\5\35\16\2\u00a2\u00a4\5!\20\2\u00a3"+
		"\u00a2\3\2\2\2\u00a3\u00a4\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5\u00a7\5\35"+
		"\16\2\u00a6\u00a3\3\2\2\2\u00a6\u00a7\3\2\2\2\u00a7\34\3\2\2\2\u00a8\u00ab"+
		"\7\62\2\2\u00a9\u00ab\5\37\17\2\u00aa\u00a8\3\2\2\2\u00aa\u00a9\3\2\2"+
		"\2\u00ab\36\3\2\2\2\u00ac\u00ad\t\6\2\2\u00ad \3\2\2\2\u00ae\u00b0\5#"+
		"\21\2\u00af\u00ae\3\2\2\2\u00b0\u00b1\3\2\2\2\u00b1\u00af\3\2\2\2\u00b1"+
		"\u00b2\3\2\2\2\u00b2\"\3\2\2\2\u00b3\u00b6\5\35\16\2\u00b4\u00b6\5\67"+
		"\33\2\u00b5\u00b3\3\2\2\2\u00b5\u00b4\3\2\2\2\u00b6$\3\2\2\2\u00b7\u00b8"+
		"\t\7\2\2\u00b8&\3\2\2\2\u00b9\u00bb\5\67\33\2\u00ba\u00b9\3\2\2\2\u00bb"+
		"\u00bc\3\2\2\2\u00bc\u00ba\3\2\2\2\u00bc\u00bd\3\2\2\2\u00bd(\3\2\2\2"+
		"\u00be\u00bf\7^\2\2\u00bf\u00c0\7w\2\2\u00c0\u00c1\5%\22\2\u00c1\u00c2"+
		"\5%\22\2\u00c2\u00c3\5%\22\2\u00c3\u00c4\5%\22\2\u00c4*\3\2\2\2\u00c5"+
		"\u00c6\7\60\2\2\u00c6,\3\2\2\2\u00c7\u00c8\7*\2\2\u00c8.\3\2\2\2\u00c9"+
		"\u00ca\7+\2\2\u00ca\60\3\2\2\2\u00cb\u00cc\7.\2\2\u00cc\62\3\2\2\2\u00cd"+
		"\u00ce\7}\2\2\u00ce\64\3\2\2\2\u00cf\u00d0\7\177\2\2\u00d0\66\3\2\2\2"+
		"\u00d1\u00d2\7a\2\2\u00d28\3\2\2\2\u00d3\u00d5\4\62;\2\u00d4\u00d3\3\2"+
		"\2\2\u00d5\u00d6\3\2\2\2\u00d6\u00d4\3\2\2\2\u00d6\u00d7\3\2\2\2\u00d7"+
		"\u00dc\3\2\2\2\u00d8\u00d9\t\b\2\2\u00d9\u00db\59\34\2\u00da\u00d8\3\2"+
		"\2\2\u00db\u00de\3\2\2\2\u00dc\u00da\3\2\2\2\u00dc\u00dd\3\2\2\2\u00dd"+
		":\3\2\2\2\u00de\u00dc\3\2\2\2\u00df\u00e1\4\62;\2\u00e0\u00df\3\2\2\2"+
		"\u00e1\u00e4\3\2\2\2\u00e2\u00e0\3\2\2\2\u00e2\u00e3\3\2\2\2\u00e3\u00e5"+
		"\3\2\2\2\u00e4\u00e2\3\2\2\2\u00e5\u00e7\7\60\2\2\u00e6\u00e8\4\62;\2"+
		"\u00e7\u00e6\3\2\2\2\u00e8\u00e9\3\2\2\2\u00e9\u00e7\3\2\2\2\u00e9\u00ea"+
		"\3\2\2\2\u00ea\u00f3\3\2\2\2\u00eb\u00ed\t\b\2\2\u00ec\u00ee\4\62;\2\u00ed"+
		"\u00ec\3\2\2\2\u00ee\u00ef\3\2\2\2\u00ef\u00ed\3\2\2\2\u00ef\u00f0\3\2"+
		"\2\2\u00f0\u00f2\3\2\2\2\u00f1\u00eb\3\2\2\2\u00f2\u00f5\3\2\2\2\u00f3"+
		"\u00f1\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4<\3\2\2\2\u00f5\u00f3\3\2\2\2"+
		"\u00f6\u00f8\t\t\2\2\u00f7\u00f6\3\2\2\2\u00f8\u00f9\3\2\2\2\u00f9\u00f7"+
		"\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u00fc\b\36\2\2"+
		"\u00fc>\3\2\2\2\u00fd\u00fe\7)\2\2\u00fe\u00ff\3\2\2\2\u00ff\u0100\b\37"+
		"\2\2\u0100@\3\2\2\2\u0101\u0103\n\n\2\2\u0102\u0101\3\2\2\2\u0103\u0104"+
		"\3\2\2\2\u0104\u0102\3\2\2\2\u0104\u0105\3\2\2\2\u0105\u0108\3\2\2\2\u0106"+
		"\u0108\5\63\31\2\u0107\u0102\3\2\2\2\u0107\u0106\3\2\2\2\u0108B\3\2\2"+
		"\2\u0109\u010a\7^\2\2\u010a\u010d\13\2\2\2\u010b\u010d\5)\24\2\u010c\u0109"+
		"\3\2\2\2\u010c\u010b\3\2\2\2\u010dD\3\2\2\2\u010e\u010f\7}\2\2\u010f\u0110"+
		"\7}\2\2\u0110\u0111\3\2\2\2\u0111\u0112\b\"\3\2\u0112F\3\2\2\2\u0113\u0114"+
		"\7&\2\2\u0114\u0115\3\2\2\2\u0115\u0116\b#\4\2\u0116H\3\2\2\2\u0117\u0118"+
		"\t\13\2\2\u0118J\3\2\2\2\u0119\u011a\t\f\2\2\u011aL\3\2\2\2\u011b\u011d"+
		"\5I$\2\u011c\u011e\5K%\2\u011d\u011c\3\2\2\2\u011e\u011f\3\2\2\2\u011f"+
		"\u011d\3\2\2\2\u011f\u0120\3\2\2\2\u0120N\3\2\2\2\u0121\u0122\5+\25\2"+
		"\u0122\u0123\3\2\2\2\u0123\u0124\b\'\5\2\u0124P\3\2\2\2\u0125\u0126\7"+
		"\177\2\2\u0126\u0127\7\177\2\2\u0127\u0128\3\2\2\2\u0128\u0129\b(\6\2"+
		"\u0129R\3\2\2\2\u012a\u012b\5G#\2\u012b\u012c\3\2\2\2\u012c\u012d\b)\7"+
		"\2\u012dT\3\2\2\2\u012e\u012f\5\5\2\2\u012f\u0130\3\2\2\2\u0130\u0131"+
		"\b*\b\2\u0131V\3\2\2\2\u0132\u0133\5M&\2\u0133\u0134\3\2\2\2\u0134\u0135"+
		"\b+\t\2\u0135X\3\2\2\2\u0136\u0137\5\61\30\2\u0137\u0138\3\2\2\2\u0138"+
		"\u0139\b,\n\2\u0139Z\3\2\2\2\u013a\u013b\5+\25\2\u013b\u013c\3\2\2\2\u013c"+
		"\u013d\b-\5\2\u013d\\\3\2\2\2\u013e\u013f\5-\26\2\u013f\u0140\3\2\2\2"+
		"\u0140\u0141\b.\13\2\u0141^\3\2\2\2\u0142\u0143\5/\27\2\u0143\u0144\3"+
		"\2\2\2\u0144\u0145\b/\f\2\u0145`\3\2\2\2\u0146\u0147\5=\36\2\u0147\u0148"+
		"\3\2\2\2\u0148\u0149\b\60\2\2\u0149b\3\2\2\2\36\2\3\4glnu\u0088\u0098"+
		"\u009d\u009f\u00a3\u00a6\u00aa\u00b1\u00b5\u00bc\u00d6\u00dc\u00e2\u00e9"+
		"\u00ef\u00f3\u00f9\u0104\u0107\u010c\u011f\r\b\2\2\7\4\2\7\3\2\t\b\2\6"+
		"\2\2\t\26\2\t\3\2\t\31\2\t\13\2\t\t\2\t\n\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}