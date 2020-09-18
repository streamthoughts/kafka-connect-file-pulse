// Generated from io/streamthoughts/kafka/connect/filepulse/expression/parser/antlr4/ScELParser.g4 by ANTLR 4.7.1
package io.streamthoughts.kafka.connect.filepulse.expression.parser.antlr4;

/*
 * Copyright 2019-2020 StreamThoughts.
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

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ScELParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.1", RuntimeMetaData.VERSION); }

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
	public static final String[] ruleNames = {
		"scel", "substitutionExpression", "substitutionStrContent", "substitutionStrExpression", 
		"expression", "propertyDeclaration", "scope", "attribute", "functionDeclaration", 
		"functionParameters", "functionObjectParameter", "value"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, "'null'", null, "'.'", "'('", "')'", "','", "'{'", 
		"'}'", "'_'", null, null, null, "'''", null, null, "'{{'", "'$'", null, 
		null, null, "'}}'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "Literal", "StringLiteral", "BooleanLiteral", "NullLiteral", "IntegerLiteral", 
		"DOT", "LPAREN", "RPAREN", "COMMA", "LBRACE", "RBRACE", "UNDER_SCORE", 
		"NUMBER", "FLOAT", "WS", "QUOTE", "LineStrText", "LineStrEscapedChar", 
		"LineSubstExprStart", "PropertyExprStart", "Letter", "LetterOrDigit", 
		"Identifier", "LineSubstExprEnd", "Substr_WS"
	};
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
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\33f\4\2\t\2\4\3\t"+
		"\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t\13\4"+
		"\f\t\f\4\r\t\r\3\2\3\2\3\2\5\2\36\n\2\3\2\3\2\3\3\3\3\7\3$\n\3\f\3\16"+
		"\3\'\13\3\3\4\3\4\3\5\3\5\3\5\6\5.\n\5\r\5\16\5/\3\5\3\5\3\6\3\6\5\6\66"+
		"\n\6\3\7\3\7\3\7\3\7\5\7<\n\7\3\7\3\7\3\7\5\7A\n\7\3\b\3\b\3\t\3\t\3\t"+
		"\7\tH\n\t\f\t\16\tK\13\t\3\n\3\n\3\n\3\13\3\13\3\13\3\13\7\13T\n\13\f"+
		"\13\16\13W\13\13\7\13Y\n\13\f\13\16\13\\\13\13\3\13\3\13\3\f\3\f\5\fb"+
		"\n\f\3\r\3\r\3\r\4IU\2\16\2\4\6\b\n\f\16\20\22\24\26\30\2\3\3\2\23\24"+
		"\2f\2\35\3\2\2\2\4%\3\2\2\2\6(\3\2\2\2\b*\3\2\2\2\n\65\3\2\2\2\f@\3\2"+
		"\2\2\16B\3\2\2\2\20D\3\2\2\2\22L\3\2\2\2\24O\3\2\2\2\26a\3\2\2\2\30c\3"+
		"\2\2\2\32\36\5\f\7\2\33\36\5\4\3\2\34\36\5\30\r\2\35\32\3\2\2\2\35\33"+
		"\3\2\2\2\35\34\3\2\2\2\36\37\3\2\2\2\37 \7\2\2\3 \3\3\2\2\2!$\5\6\4\2"+
		"\"$\5\b\5\2#!\3\2\2\2#\"\3\2\2\2$\'\3\2\2\2%#\3\2\2\2%&\3\2\2\2&\5\3\2"+
		"\2\2\'%\3\2\2\2()\t\2\2\2)\7\3\2\2\2*-\7\25\2\2+.\5\n\6\2,.\5\30\r\2-"+
		"+\3\2\2\2-,\3\2\2\2./\3\2\2\2/-\3\2\2\2/\60\3\2\2\2\60\61\3\2\2\2\61\62"+
		"\7\32\2\2\62\t\3\2\2\2\63\66\5\f\7\2\64\66\5\22\n\2\65\63\3\2\2\2\65\64"+
		"\3\2\2\2\66\13\3\2\2\2\678\7\26\2\28;\5\16\b\29:\7\b\2\2:<\5\20\t\2;9"+
		"\3\2\2\2;<\3\2\2\2<A\3\2\2\2=>\7\26\2\2>?\7\b\2\2?A\5\20\t\2@\67\3\2\2"+
		"\2@=\3\2\2\2A\r\3\2\2\2BC\7\31\2\2C\17\3\2\2\2DI\7\31\2\2EF\7\b\2\2FH"+
		"\7\31\2\2GE\3\2\2\2HK\3\2\2\2IJ\3\2\2\2IG\3\2\2\2J\21\3\2\2\2KI\3\2\2"+
		"\2LM\7\31\2\2MN\5\24\13\2N\23\3\2\2\2OZ\7\t\2\2PU\5\26\f\2QR\7\13\2\2"+
		"RT\5\26\f\2SQ\3\2\2\2TW\3\2\2\2UV\3\2\2\2US\3\2\2\2VY\3\2\2\2WU\3\2\2"+
		"\2XP\3\2\2\2Y\\\3\2\2\2ZX\3\2\2\2Z[\3\2\2\2[]\3\2\2\2\\Z\3\2\2\2]^\7\n"+
		"\2\2^\25\3\2\2\2_b\5\n\6\2`b\5\30\r\2a_\3\2\2\2a`\3\2\2\2b\27\3\2\2\2"+
		"cd\7\3\2\2d\31\3\2\2\2\16\35#%-/\65;@IUZa";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}