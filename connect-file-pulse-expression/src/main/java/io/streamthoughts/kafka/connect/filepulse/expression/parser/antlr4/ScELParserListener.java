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

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link ScELParser}.
 */
public interface ScELParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link ScELParser#scel}.
	 * @param ctx the parse tree
	 */
	void enterScel(ScELParser.ScelContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#scel}.
	 * @param ctx the parse tree
	 */
	void exitScel(ScELParser.ScelContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#substitutionExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubstitutionExpression(ScELParser.SubstitutionExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#substitutionExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubstitutionExpression(ScELParser.SubstitutionExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#substitutionStrContent}.
	 * @param ctx the parse tree
	 */
	void enterSubstitutionStrContent(ScELParser.SubstitutionStrContentContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#substitutionStrContent}.
	 * @param ctx the parse tree
	 */
	void exitSubstitutionStrContent(ScELParser.SubstitutionStrContentContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#substitutionStrExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubstitutionStrExpression(ScELParser.SubstitutionStrExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#substitutionStrExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubstitutionStrExpression(ScELParser.SubstitutionStrExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(ScELParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(ScELParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#propertyDeclaration}.
	 * @param ctx the parse tree
	 */
	void enterPropertyDeclaration(ScELParser.PropertyDeclarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#propertyDeclaration}.
	 * @param ctx the parse tree
	 */
	void exitPropertyDeclaration(ScELParser.PropertyDeclarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#scope}.
	 * @param ctx the parse tree
	 */
	void enterScope(ScELParser.ScopeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#scope}.
	 * @param ctx the parse tree
	 */
	void exitScope(ScELParser.ScopeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#attribute}.
	 * @param ctx the parse tree
	 */
	void enterAttribute(ScELParser.AttributeContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#attribute}.
	 * @param ctx the parse tree
	 */
	void exitAttribute(ScELParser.AttributeContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#functionDeclaration}.
	 * @param ctx the parse tree
	 */
	void enterFunctionDeclaration(ScELParser.FunctionDeclarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#functionDeclaration}.
	 * @param ctx the parse tree
	 */
	void exitFunctionDeclaration(ScELParser.FunctionDeclarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#functionParameters}.
	 * @param ctx the parse tree
	 */
	void enterFunctionParameters(ScELParser.FunctionParametersContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#functionParameters}.
	 * @param ctx the parse tree
	 */
	void exitFunctionParameters(ScELParser.FunctionParametersContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#functionObjectParameter}.
	 * @param ctx the parse tree
	 */
	void enterFunctionObjectParameter(ScELParser.FunctionObjectParameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#functionObjectParameter}.
	 * @param ctx the parse tree
	 */
	void exitFunctionObjectParameter(ScELParser.FunctionObjectParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link ScELParser#value}.
	 * @param ctx the parse tree
	 */
	void enterValue(ScELParser.ValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ScELParser#value}.
	 * @param ctx the parse tree
	 */
	void exitValue(ScELParser.ValueContext ctx);
}