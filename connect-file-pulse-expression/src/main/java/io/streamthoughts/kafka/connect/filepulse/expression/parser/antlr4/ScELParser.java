/*
 * Copyright 2023 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.expression.parser.antlr4;

/*
 * Copyright 2022 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.List;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ScELParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.10.1", RuntimeMetaData.VERSION); }

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
		RULE_scel = 0, RULE_substitutionExpression = 1, RULE_substitutionStrContent = 2, 
		RULE_substitutionStrExpression = 3, RULE_expression = 4, RULE_propertyDeclaration = 5, 
		RULE_scope = 6, RULE_attribute = 7, RULE_functionDeclaration = 8, RULE_functionParameters = 9, 
		RULE_functionObjectParameter = 10, RULE_value = 11;
	private static String[] makeRuleNames() {
		return new String[] {
			"scel", "substitutionExpression", "substitutionStrContent", "substitutionStrExpression", 
			"expression", "propertyDeclaration", "scope", "attribute", "functionDeclaration", 
			"functionParameters", "functionObjectParameter", "value"
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

	@Override
	public String getGrammarFileName() { return "ScELParser.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public ScELParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class ScelContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(ScELParser.EOF, 0); }
		public PropertyDeclarationContext propertyDeclaration() {
			return getRuleContext(PropertyDeclarationContext.class,0);
		}
		public SubstitutionExpressionContext substitutionExpression() {
			return getRuleContext(SubstitutionExpressionContext.class,0);
		}
		public ValueContext value() {
			return getRuleContext(ValueContext.class,0);
		}
		public ScelContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_scel; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterScel(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitScel(this);
		}
	}

	public final ScelContext scel() throws RecognitionException {
		ScelContext _localctx = new ScelContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_scel);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(27);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PropertyExprStart:
				{
				setState(24);
				propertyDeclaration();
				}
				break;
			case EOF:
			case LineStrText:
			case LineStrEscapedChar:
			case LineSubstExprStart:
				{
				setState(25);
				substitutionExpression();
				}
				break;
			case Literal:
				{
				setState(26);
				value();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(29);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubstitutionExpressionContext extends ParserRuleContext {
		public List<SubstitutionStrContentContext> substitutionStrContent() {
			return getRuleContexts(SubstitutionStrContentContext.class);
		}
		public SubstitutionStrContentContext substitutionStrContent(int i) {
			return getRuleContext(SubstitutionStrContentContext.class,i);
		}
		public List<SubstitutionStrExpressionContext> substitutionStrExpression() {
			return getRuleContexts(SubstitutionStrExpressionContext.class);
		}
		public SubstitutionStrExpressionContext substitutionStrExpression(int i) {
			return getRuleContext(SubstitutionStrExpressionContext.class,i);
		}
		public SubstitutionExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_substitutionExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterSubstitutionExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitSubstitutionExpression(this);
		}
	}

	public final SubstitutionExpressionContext substitutionExpression() throws RecognitionException {
		SubstitutionExpressionContext _localctx = new SubstitutionExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_substitutionExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(35);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LineStrText) | (1L << LineStrEscapedChar) | (1L << LineSubstExprStart))) != 0)) {
				{
				setState(33);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LineStrText:
				case LineStrEscapedChar:
					{
					setState(31);
					substitutionStrContent();
					}
					break;
				case LineSubstExprStart:
					{
					setState(32);
					substitutionStrExpression();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(37);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubstitutionStrContentContext extends ParserRuleContext {
		public TerminalNode LineStrText() { return getToken(ScELParser.LineStrText, 0); }
		public TerminalNode LineStrEscapedChar() { return getToken(ScELParser.LineStrEscapedChar, 0); }
		public SubstitutionStrContentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_substitutionStrContent; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterSubstitutionStrContent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitSubstitutionStrContent(this);
		}
	}

	public final SubstitutionStrContentContext substitutionStrContent() throws RecognitionException {
		SubstitutionStrContentContext _localctx = new SubstitutionStrContentContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_substitutionStrContent);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(38);
			_la = _input.LA(1);
			if ( !(_la==LineStrText || _la==LineStrEscapedChar) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubstitutionStrExpressionContext extends ParserRuleContext {
		public TerminalNode LineSubstExprStart() { return getToken(ScELParser.LineSubstExprStart, 0); }
		public TerminalNode LineSubstExprEnd() { return getToken(ScELParser.LineSubstExprEnd, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<ValueContext> value() {
			return getRuleContexts(ValueContext.class);
		}
		public ValueContext value(int i) {
			return getRuleContext(ValueContext.class,i);
		}
		public SubstitutionStrExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_substitutionStrExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterSubstitutionStrExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitSubstitutionStrExpression(this);
		}
	}

	public final SubstitutionStrExpressionContext substitutionStrExpression() throws RecognitionException {
		SubstitutionStrExpressionContext _localctx = new SubstitutionStrExpressionContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_substitutionStrExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(40);
			match(LineSubstExprStart);
			setState(43); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				setState(43);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case PropertyExprStart:
				case Identifier:
					{
					setState(41);
					expression();
					}
					break;
				case Literal:
					{
					setState(42);
					value();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(45); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << Literal) | (1L << PropertyExprStart) | (1L << Identifier))) != 0) );
			setState(47);
			match(LineSubstExprEnd);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public PropertyDeclarationContext propertyDeclaration() {
			return getRuleContext(PropertyDeclarationContext.class,0);
		}
		public FunctionDeclarationContext functionDeclaration() {
			return getRuleContext(FunctionDeclarationContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitExpression(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_expression);
		try {
			setState(51);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PropertyExprStart:
				enterOuterAlt(_localctx, 1);
				{
				setState(49);
				propertyDeclaration();
				}
				break;
			case Identifier:
				enterOuterAlt(_localctx, 2);
				{
				setState(50);
				functionDeclaration();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PropertyDeclarationContext extends ParserRuleContext {
		public TerminalNode PropertyExprStart() { return getToken(ScELParser.PropertyExprStart, 0); }
		public ScopeContext scope() {
			return getRuleContext(ScopeContext.class,0);
		}
		public TerminalNode DOT() { return getToken(ScELParser.DOT, 0); }
		public AttributeContext attribute() {
			return getRuleContext(AttributeContext.class,0);
		}
		public PropertyDeclarationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyDeclaration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterPropertyDeclaration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitPropertyDeclaration(this);
		}
	}

	public final PropertyDeclarationContext propertyDeclaration() throws RecognitionException {
		PropertyDeclarationContext _localctx = new PropertyDeclarationContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_propertyDeclaration);
		int _la;
		try {
			setState(62);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(53);
				match(PropertyExprStart);
				setState(54);
				scope();
				setState(57);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DOT) {
					{
					setState(55);
					match(DOT);
					setState(56);
					attribute();
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(59);
				match(PropertyExprStart);
				setState(60);
				match(DOT);
				setState(61);
				attribute();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ScopeContext extends ParserRuleContext {
		public TerminalNode Identifier() { return getToken(ScELParser.Identifier, 0); }
		public ScopeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_scope; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterScope(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitScope(this);
		}
	}

	public final ScopeContext scope() throws RecognitionException {
		ScopeContext _localctx = new ScopeContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_scope);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(64);
			match(Identifier);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AttributeContext extends ParserRuleContext {
		public List<TerminalNode> Identifier() { return getTokens(ScELParser.Identifier); }
		public TerminalNode Identifier(int i) {
			return getToken(ScELParser.Identifier, i);
		}
		public List<TerminalNode> DOT() { return getTokens(ScELParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ScELParser.DOT, i);
		}
		public AttributeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_attribute; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterAttribute(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitAttribute(this);
		}
	}

	public final AttributeContext attribute() throws RecognitionException {
		AttributeContext _localctx = new AttributeContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_attribute);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(66);
			match(Identifier);
			setState(71);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
			while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(67);
					match(DOT);
					setState(68);
					match(Identifier);
					}
					} 
				}
				setState(73);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionDeclarationContext extends ParserRuleContext {
		public TerminalNode Identifier() { return getToken(ScELParser.Identifier, 0); }
		public FunctionParametersContext functionParameters() {
			return getRuleContext(FunctionParametersContext.class,0);
		}
		public FunctionDeclarationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionDeclaration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterFunctionDeclaration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitFunctionDeclaration(this);
		}
	}

	public final FunctionDeclarationContext functionDeclaration() throws RecognitionException {
		FunctionDeclarationContext _localctx = new FunctionDeclarationContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_functionDeclaration);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(74);
			match(Identifier);
			setState(75);
			functionParameters();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionParametersContext extends ParserRuleContext {
		public TerminalNode LPAREN() { return getToken(ScELParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(ScELParser.RPAREN, 0); }
		public List<FunctionObjectParameterContext> functionObjectParameter() {
			return getRuleContexts(FunctionObjectParameterContext.class);
		}
		public FunctionObjectParameterContext functionObjectParameter(int i) {
			return getRuleContext(FunctionObjectParameterContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(ScELParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ScELParser.COMMA, i);
		}
		public FunctionParametersContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionParameters; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterFunctionParameters(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitFunctionParameters(this);
		}
	}

	public final FunctionParametersContext functionParameters() throws RecognitionException {
		FunctionParametersContext _localctx = new FunctionParametersContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_functionParameters);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(77);
			match(LPAREN);
			setState(88);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << Literal) | (1L << PropertyExprStart) | (1L << Identifier))) != 0)) {
				{
				{
				setState(78);
				functionObjectParameter();
				setState(83);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(79);
						match(COMMA);
						setState(80);
						functionObjectParameter();
						}
						} 
					}
					setState(85);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
				}
				}
				}
				setState(90);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(91);
			match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionObjectParameterContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ValueContext value() {
			return getRuleContext(ValueContext.class,0);
		}
		public FunctionObjectParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionObjectParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterFunctionObjectParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitFunctionObjectParameter(this);
		}
	}

	public final FunctionObjectParameterContext functionObjectParameter() throws RecognitionException {
		FunctionObjectParameterContext _localctx = new FunctionObjectParameterContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_functionObjectParameter);
		try {
			setState(95);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PropertyExprStart:
			case Identifier:
				enterOuterAlt(_localctx, 1);
				{
				setState(93);
				expression();
				}
				break;
			case Literal:
				enterOuterAlt(_localctx, 2);
				{
				setState(94);
				value();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueContext extends ParserRuleContext {
		public TerminalNode Literal() { return getToken(ScELParser.Literal, 0); }
		public ValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).enterValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ScELParserListener ) ((ScELParserListener)listener).exitValue(this);
		}
	}

	public final ValueContext value() throws RecognitionException {
		ValueContext _localctx = new ValueContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(97);
			match(Literal);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\u0004\u0001\u0019d\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0001"+
		"\u0000\u0001\u0000\u0001\u0000\u0003\u0000\u001c\b\u0000\u0001\u0000\u0001"+
		"\u0000\u0001\u0001\u0001\u0001\u0005\u0001\"\b\u0001\n\u0001\f\u0001%"+
		"\t\u0001\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0004"+
		"\u0003,\b\u0003\u000b\u0003\f\u0003-\u0001\u0003\u0001\u0003\u0001\u0004"+
		"\u0001\u0004\u0003\u00044\b\u0004\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005:\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005?\b\u0005\u0001\u0006\u0001\u0006\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0005\u0007F\b\u0007\n\u0007\f\u0007I\t\u0007\u0001\b\u0001"+
		"\b\u0001\b\u0001\t\u0001\t\u0001\t\u0001\t\u0005\tR\b\t\n\t\f\tU\t\t\u0005"+
		"\tW\b\t\n\t\f\tZ\t\t\u0001\t\u0001\t\u0001\n\u0001\n\u0003\n`\b\n\u0001"+
		"\u000b\u0001\u000b\u0001\u000b\u0002GS\u0000\f\u0000\u0002\u0004\u0006"+
		"\b\n\f\u000e\u0010\u0012\u0014\u0016\u0000\u0001\u0001\u0000\u0011\u0012"+
		"d\u0000\u001b\u0001\u0000\u0000\u0000\u0002#\u0001\u0000\u0000\u0000\u0004"+
		"&\u0001\u0000\u0000\u0000\u0006(\u0001\u0000\u0000\u0000\b3\u0001\u0000"+
		"\u0000\u0000\n>\u0001\u0000\u0000\u0000\f@\u0001\u0000\u0000\u0000\u000e"+
		"B\u0001\u0000\u0000\u0000\u0010J\u0001\u0000\u0000\u0000\u0012M\u0001"+
		"\u0000\u0000\u0000\u0014_\u0001\u0000\u0000\u0000\u0016a\u0001\u0000\u0000"+
		"\u0000\u0018\u001c\u0003\n\u0005\u0000\u0019\u001c\u0003\u0002\u0001\u0000"+
		"\u001a\u001c\u0003\u0016\u000b\u0000\u001b\u0018\u0001\u0000\u0000\u0000"+
		"\u001b\u0019\u0001\u0000\u0000\u0000\u001b\u001a\u0001\u0000\u0000\u0000"+
		"\u001c\u001d\u0001\u0000\u0000\u0000\u001d\u001e\u0005\u0000\u0000\u0001"+
		"\u001e\u0001\u0001\u0000\u0000\u0000\u001f\"\u0003\u0004\u0002\u0000 "+
		"\"\u0003\u0006\u0003\u0000!\u001f\u0001\u0000\u0000\u0000! \u0001\u0000"+
		"\u0000\u0000\"%\u0001\u0000\u0000\u0000#!\u0001\u0000\u0000\u0000#$\u0001"+
		"\u0000\u0000\u0000$\u0003\u0001\u0000\u0000\u0000%#\u0001\u0000\u0000"+
		"\u0000&\'\u0007\u0000\u0000\u0000\'\u0005\u0001\u0000\u0000\u0000(+\u0005"+
		"\u0013\u0000\u0000),\u0003\b\u0004\u0000*,\u0003\u0016\u000b\u0000+)\u0001"+
		"\u0000\u0000\u0000+*\u0001\u0000\u0000\u0000,-\u0001\u0000\u0000\u0000"+
		"-+\u0001\u0000\u0000\u0000-.\u0001\u0000\u0000\u0000./\u0001\u0000\u0000"+
		"\u0000/0\u0005\u0018\u0000\u00000\u0007\u0001\u0000\u0000\u000014\u0003"+
		"\n\u0005\u000024\u0003\u0010\b\u000031\u0001\u0000\u0000\u000032\u0001"+
		"\u0000\u0000\u00004\t\u0001\u0000\u0000\u000056\u0005\u0014\u0000\u0000"+
		"69\u0003\f\u0006\u000078\u0005\u0006\u0000\u00008:\u0003\u000e\u0007\u0000"+
		"97\u0001\u0000\u0000\u00009:\u0001\u0000\u0000\u0000:?\u0001\u0000\u0000"+
		"\u0000;<\u0005\u0014\u0000\u0000<=\u0005\u0006\u0000\u0000=?\u0003\u000e"+
		"\u0007\u0000>5\u0001\u0000\u0000\u0000>;\u0001\u0000\u0000\u0000?\u000b"+
		"\u0001\u0000\u0000\u0000@A\u0005\u0017\u0000\u0000A\r\u0001\u0000\u0000"+
		"\u0000BG\u0005\u0017\u0000\u0000CD\u0005\u0006\u0000\u0000DF\u0005\u0017"+
		"\u0000\u0000EC\u0001\u0000\u0000\u0000FI\u0001\u0000\u0000\u0000GH\u0001"+
		"\u0000\u0000\u0000GE\u0001\u0000\u0000\u0000H\u000f\u0001\u0000\u0000"+
		"\u0000IG\u0001\u0000\u0000\u0000JK\u0005\u0017\u0000\u0000KL\u0003\u0012"+
		"\t\u0000L\u0011\u0001\u0000\u0000\u0000MX\u0005\u0007\u0000\u0000NS\u0003"+
		"\u0014\n\u0000OP\u0005\t\u0000\u0000PR\u0003\u0014\n\u0000QO\u0001\u0000"+
		"\u0000\u0000RU\u0001\u0000\u0000\u0000ST\u0001\u0000\u0000\u0000SQ\u0001"+
		"\u0000\u0000\u0000TW\u0001\u0000\u0000\u0000US\u0001\u0000\u0000\u0000"+
		"VN\u0001\u0000\u0000\u0000WZ\u0001\u0000\u0000\u0000XV\u0001\u0000\u0000"+
		"\u0000XY\u0001\u0000\u0000\u0000Y[\u0001\u0000\u0000\u0000ZX\u0001\u0000"+
		"\u0000\u0000[\\\u0005\b\u0000\u0000\\\u0013\u0001\u0000\u0000\u0000]`"+
		"\u0003\b\u0004\u0000^`\u0003\u0016\u000b\u0000_]\u0001\u0000\u0000\u0000"+
		"_^\u0001\u0000\u0000\u0000`\u0015\u0001\u0000\u0000\u0000ab\u0005\u0001"+
		"\u0000\u0000b\u0017\u0001\u0000\u0000\u0000\f\u001b!#+-39>GSX_";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}