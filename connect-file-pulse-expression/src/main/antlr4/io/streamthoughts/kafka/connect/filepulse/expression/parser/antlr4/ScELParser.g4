parser grammar ScELParser;

options { tokenVocab = ScELLexer; }

@header {
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
}

scel
    : ( propertyDeclaration | substitutionExpression | value ) EOF
    ;

substitutionExpression
    : (substitutionStrContent | substitutionStrExpression)*
    ;

substitutionStrContent
    :   LineStrText
    |   LineStrEscapedChar
    ;

substitutionStrExpression
    :   LineSubstExprStart (expression | value)+ LineSubstExprEnd
    ;

expression
    :   propertyDeclaration
    |   functionDeclaration
    ;

// Property
propertyDeclaration
    :   PropertyExprStart scope (DOT attribute)?
    |   PropertyExprStart DOT attribute
    ;

scope
    :   Identifier
    ;

attribute
    :   Identifier (DOT Identifier)*?
    ;

// Function
functionDeclaration
    :   Identifier functionParameters
    ;

functionParameters
    :   LPAREN functionObjectParameter (COMMA value)* RPAREN
    ;

functionObjectParameter
    :   expression
    ;


// Value
value
    :   Literal
    ;