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

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link ScELParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface ScELParserVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link ScELParser#scel}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitScel(ScELParser.ScelContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#substitutionExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubstitutionExpression(ScELParser.SubstitutionExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#substitutionStrContent}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubstitutionStrContent(ScELParser.SubstitutionStrContentContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#substitutionStrExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubstitutionStrExpression(ScELParser.SubstitutionStrExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(ScELParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#propertyDeclaration}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPropertyDeclaration(ScELParser.PropertyDeclarationContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#scope}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitScope(ScELParser.ScopeContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#attribute}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAttribute(ScELParser.AttributeContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#functionDeclaration}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionDeclaration(ScELParser.FunctionDeclarationContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#functionParameters}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionParameters(ScELParser.FunctionParametersContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#functionObjectParameter}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionObjectParameter(ScELParser.FunctionObjectParameterContext ctx);
	/**
	 * Visit a parse tree produced by {@link ScELParser#value}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValue(ScELParser.ValueContext ctx);
}