// Generated from /home/florian/Workspace/stream-thoughts/GitHub/kafka-connect-file-pulse/connect-file-pulse-expression/src/main/antlr4/io/streamthoughts/kafka/connect/filepulse/expression/parser/antlr4/ScELParser.g4 by ANTLR 4.8

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
	static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		Literal=1, StringLiteral=2, BooleanLiteral=3, NullLiteral=4, IntegerLiteral=5, 
		DOT=6, LPAREN=7, RPAREN=8, COMMA=9, LBRACE=10, RBRACE=11, UNDER_SCORE=12, 
		Letter=13, LetterOrDigit=14, NUMBER=15, FLOAT=16, WS=17, QUOTE=18, LineStrText=19, 
		LineStrEscapedChar=20, LinePropertyExprStart=21, ScopeIdentifier=22, DotAttributeIdentifier=23, 
		Identifier=24, LineSubstExprStart=25, LineSubstExprEnd=26, Substr_WS=27;
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
			"'}'", "'_'", null, null, null, null, null, "'''", null, null, "'$'", 
			null, null, null, "'{{'", "'}}'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "Literal", "StringLiteral", "BooleanLiteral", "NullLiteral", "IntegerLiteral", 
			"DOT", "LPAREN", "RPAREN", "COMMA", "LBRACE", "RBRACE", "UNDER_SCORE", 
			"Letter", "LetterOrDigit", "NUMBER", "FLOAT", "WS", "QUOTE", "LineStrText", 
			"LineStrEscapedChar", "LinePropertyExprStart", "ScopeIdentifier", "DotAttributeIdentifier", 
			"Identifier", "LineSubstExprStart", "LineSubstExprEnd", "Substr_WS"
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitScel(this);
			else return visitor.visitChildren(this);
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
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				{
				setState(24);
				propertyDeclaration();
				}
				break;
			case 2:
				{
				setState(25);
				substitutionExpression();
				}
				break;
			case 3:
				{
				setState(26);
				value();
				}
				break;
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitSubstitutionExpression(this);
			else return visitor.visitChildren(this);
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
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LineStrText) | (1L << LineStrEscapedChar) | (1L << LinePropertyExprStart) | (1L << LineSubstExprStart))) != 0)) {
				{
				setState(33);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LineStrText:
				case LineStrEscapedChar:
				case LinePropertyExprStart:
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
		public PropertyDeclarationContext propertyDeclaration() {
			return getRuleContext(PropertyDeclarationContext.class,0);
		}
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitSubstitutionStrContent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubstitutionStrContentContext substitutionStrContent() throws RecognitionException {
		SubstitutionStrContentContext _localctx = new SubstitutionStrContentContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_substitutionStrContent);
		try {
			setState(41);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LineStrText:
				enterOuterAlt(_localctx, 1);
				{
				setState(38);
				match(LineStrText);
				}
				break;
			case LineStrEscapedChar:
				enterOuterAlt(_localctx, 2);
				{
				setState(39);
				match(LineStrEscapedChar);
				}
				break;
			case LinePropertyExprStart:
				enterOuterAlt(_localctx, 3);
				{
				setState(40);
				propertyDeclaration();
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

	public static class SubstitutionStrExpressionContext extends ParserRuleContext {
		public TerminalNode LineSubstExprStart() { return getToken(ScELParser.LineSubstExprStart, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode LineSubstExprEnd() { return getToken(ScELParser.LineSubstExprEnd, 0); }
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitSubstitutionStrExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubstitutionStrExpressionContext substitutionStrExpression() throws RecognitionException {
		SubstitutionStrExpressionContext _localctx = new SubstitutionStrExpressionContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_substitutionStrExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(43);
			match(LineSubstExprStart);
			setState(44);
			expression();
			setState(45);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_expression);
		try {
			setState(49);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LinePropertyExprStart:
				enterOuterAlt(_localctx, 1);
				{
				setState(47);
				propertyDeclaration();
				}
				break;
			case Identifier:
				enterOuterAlt(_localctx, 2);
				{
				setState(48);
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
		public TerminalNode LinePropertyExprStart() { return getToken(ScELParser.LinePropertyExprStart, 0); }
		public ScopeContext scope() {
			return getRuleContext(ScopeContext.class,0);
		}
		public List<TerminalNode> DOT() { return getTokens(ScELParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ScELParser.DOT, i);
		}
		public List<AttributeContext> attribute() {
			return getRuleContexts(AttributeContext.class);
		}
		public AttributeContext attribute(int i) {
			return getRuleContext(AttributeContext.class,i);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitPropertyDeclaration(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyDeclarationContext propertyDeclaration() throws RecognitionException {
		PropertyDeclarationContext _localctx = new PropertyDeclarationContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_propertyDeclaration);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(51);
			match(LinePropertyExprStart);
			setState(52);
			scope();
			setState(55); 
			_errHandler.sync(this);
			_alt = 1+1;
			do {
				switch (_alt) {
				case 1+1:
					{
					{
					setState(53);
					match(DOT);
					setState(54);
					attribute();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(57); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,5,_ctx);
			} while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitScope(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ScopeContext scope() throws RecognitionException {
		ScopeContext _localctx = new ScopeContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_scope);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(59);
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
		public TerminalNode Identifier() { return getToken(ScELParser.Identifier, 0); }
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitAttribute(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AttributeContext attribute() throws RecognitionException {
		AttributeContext _localctx = new AttributeContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_attribute);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(61);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitFunctionDeclaration(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionDeclarationContext functionDeclaration() throws RecognitionException {
		FunctionDeclarationContext _localctx = new FunctionDeclarationContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_functionDeclaration);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(63);
			match(Identifier);
			setState(64);
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
		public FunctionObjectParameterContext functionObjectParameter() {
			return getRuleContext(FunctionObjectParameterContext.class,0);
		}
		public TerminalNode RPAREN() { return getToken(ScELParser.RPAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(ScELParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ScELParser.COMMA, i);
		}
		public List<ValueContext> value() {
			return getRuleContexts(ValueContext.class);
		}
		public ValueContext value(int i) {
			return getRuleContext(ValueContext.class,i);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitFunctionParameters(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionParametersContext functionParameters() throws RecognitionException {
		FunctionParametersContext _localctx = new FunctionParametersContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_functionParameters);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(66);
			match(LPAREN);
			setState(67);
			functionObjectParameter();
			setState(72);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(68);
				match(COMMA);
				setState(69);
				value();
				}
				}
				setState(74);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(75);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitFunctionObjectParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionObjectParameterContext functionObjectParameter() throws RecognitionException {
		FunctionObjectParameterContext _localctx = new FunctionObjectParameterContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_functionObjectParameter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(77);
			expression();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ScELParserVisitor ) return ((ScELParserVisitor<? extends T>)visitor).visitValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueContext value() throws RecognitionException {
		ValueContext _localctx = new ValueContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(79);
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
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\35T\4\2\t\2\4\3\t"+
		"\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t\13\4"+
		"\f\t\f\4\r\t\r\3\2\3\2\3\2\5\2\36\n\2\3\2\3\2\3\3\3\3\7\3$\n\3\f\3\16"+
		"\3\'\13\3\3\4\3\4\3\4\5\4,\n\4\3\5\3\5\3\5\3\5\3\6\3\6\5\6\64\n\6\3\7"+
		"\3\7\3\7\3\7\6\7:\n\7\r\7\16\7;\3\b\3\b\3\t\3\t\3\n\3\n\3\n\3\13\3\13"+
		"\3\13\3\13\7\13I\n\13\f\13\16\13L\13\13\3\13\3\13\3\f\3\f\3\r\3\r\3\r"+
		"\3;\2\16\2\4\6\b\n\f\16\20\22\24\26\30\2\2\2P\2\35\3\2\2\2\4%\3\2\2\2"+
		"\6+\3\2\2\2\b-\3\2\2\2\n\63\3\2\2\2\f\65\3\2\2\2\16=\3\2\2\2\20?\3\2\2"+
		"\2\22A\3\2\2\2\24D\3\2\2\2\26O\3\2\2\2\30Q\3\2\2\2\32\36\5\f\7\2\33\36"+
		"\5\4\3\2\34\36\5\30\r\2\35\32\3\2\2\2\35\33\3\2\2\2\35\34\3\2\2\2\36\37"+
		"\3\2\2\2\37 \7\2\2\3 \3\3\2\2\2!$\5\6\4\2\"$\5\b\5\2#!\3\2\2\2#\"\3\2"+
		"\2\2$\'\3\2\2\2%#\3\2\2\2%&\3\2\2\2&\5\3\2\2\2\'%\3\2\2\2(,\7\25\2\2)"+
		",\7\26\2\2*,\5\f\7\2+(\3\2\2\2+)\3\2\2\2+*\3\2\2\2,\7\3\2\2\2-.\7\33\2"+
		"\2./\5\n\6\2/\60\7\34\2\2\60\t\3\2\2\2\61\64\5\f\7\2\62\64\5\22\n\2\63"+
		"\61\3\2\2\2\63\62\3\2\2\2\64\13\3\2\2\2\65\66\7\27\2\2\669\5\16\b\2\67"+
		"8\7\b\2\28:\5\20\t\29\67\3\2\2\2:;\3\2\2\2;<\3\2\2\2;9\3\2\2\2<\r\3\2"+
		"\2\2=>\7\32\2\2>\17\3\2\2\2?@\7\32\2\2@\21\3\2\2\2AB\7\32\2\2BC\5\24\13"+
		"\2C\23\3\2\2\2DE\7\t\2\2EJ\5\26\f\2FG\7\13\2\2GI\5\30\r\2HF\3\2\2\2IL"+
		"\3\2\2\2JH\3\2\2\2JK\3\2\2\2KM\3\2\2\2LJ\3\2\2\2MN\7\n\2\2N\25\3\2\2\2"+
		"OP\5\n\6\2P\27\3\2\2\2QR\7\3\2\2R\31\3\2\2\2\t\35#%+\63;J";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}