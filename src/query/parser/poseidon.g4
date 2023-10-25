grammar poseidon;

// Parser rules
query : query_operator EOF ;
query_operator : filter_op 
        | node_scan_op 
        | index_scan_op
        | match_op 
        | project_op 
        | limit_op
        | crossjoin_op
        | hashjoin_op
        | leftouterjoin_op
        | foreach_relationship_op
        | expand_op
        | aggregate_op
        | group_by_op
        | union_op
        | sort_op
        | create_op
        ;

// Scan
node_scan_op : Nodescan_ '(' scan_param? ')' ;
scan_param : STRING_ 
           | scan_list
           ;
scan_list : '[' STRING_ (',' STRING_)* ']' ;

index_scan_op : Indexscan_ '(' index_scan_param ')' ;
index_scan_param : STRING_ ',' STRING_ ',' value ;

// Project
project_op : Project_ '(' proj_list ',' query_operator ')' ;
proj_list : '[' proj_expr (',' proj_expr)* ']' ;
proj_expr : function_call
        | Var ('.' Identifier_)? ':' type_spec 
        ;
type_spec : IntType_ | DoubleType_ | Uint64Type_ | StringType_ | DateType_ | ResultType_ ;

// Limit
limit_op : Limit_ '(' INTEGER ',' query_operator ')' ;

// CrossJoin
crossjoin_op : CrossJoin_ '(' query_operator ',' query_operator ')' ;

// HashJoin
hashjoin_op : HashJoin_ '(' logical_expr ',' query_operator ',' query_operator ')' ;

// LeftOuterJoin
leftouterjoin_op : LeftOuterJoin_ '(' logical_expr ',' query_operator ',' query_operator ')' ;

// ForeachRelationship
foreach_relationship_op : ForeachRelationship_ '(' rship_dir ',' STRING_ (',' rship_cardinality)?  (',' rship_source_var)? ',' query_operator ')' ;
rship_dir : FromDir_ | ToDir_ | AllDir_;
rship_cardinality : INTEGER ',' INTEGER ;
rship_source_var : Var ;

// Expand
expand_op : Expand_ '(' expand_dir ',' STRING_ ',' query_operator ')' ;
expand_dir : InExpandDir_ | OutExpandDir_ ;

// Match
match_op : Match_ '(' path_pattern ')' ;
path_pattern : node_pattern path_component* ;
path_component : rship_pattern node_pattern ; 
node_pattern : '(' (Identifier_)? ':' Identifier_ property_list? ')' ;
rship_pattern : dir_spec '[' (Identifier_)? ':' Identifier_ (cardinality_spec)? ']' dir_spec ;

cardinality_spec : '*' min_cardinality '..' (max_cardinality)? ;
min_cardinality : INTEGER ;
max_cardinality : INTEGER ;

dir_spec: left_dir | right_dir | no_dir ;
left_dir : '<-' ;
right_dir : '->' ;
no_dir    : '-' ;

// Aggregate
aggregate_op : Aggregate_ '(' aggregate_list ',' query_operator ')' ;
aggregate_list : '[' aggr_expr (',' aggr_expr)*']' ;
aggr_expr : aggr_func '(' proj_expr ')';
aggr_func : Count_ | Sum_ | Avg_ | Min_ | Max_ ;

// Union
union_op : Union_ '(' query_operator ',' query_operator ')';

// GroupBy
group_by_op : GroupBy_ '(' grouping_list ',' aggregate_list ',' query_operator ')' ;
grouping_list : '[' grouping_expr (',' grouping_expr)* ']' ;
grouping_expr : Var ('.' Identifier_)? ':' type_spec ;

// Filter
filter_op : Filter_ '(' logical_expr ',' query_operator ')' ;
logical_expr : boolean_expr ( OR boolean_expr )* ;
boolean_expr : equality_expr ( AND equality_expr )* ;
equality_expr : relational_expr ( (EQUALS | NOTEQUALS) relational_expr)* ;
relational_expr : additive_expr ( (LT | LTEQ | GT | GTEQ) additive_expr)* ;
additive_expr : multiplicative_expr ( (PLUS_ | '-') multiplicative_expr )* ;
multiplicative_expr : unary_expr (( MULT | DIV | MOD ) unary_expr)* ;
unary_expr : NOT? primary_expr ;
primary_expr : '(' logical_expr ')'
        | function_call
        | value
        | variable
        ;

variable : Var '.' Identifier_ ;

value   : INTEGER
        | FLOAT
        | STRING_
        ;

function_call : udf_prefix Identifier_ '(' param_list? ')' ;
udf_prefix    : UDF_ DOUBLE_COLON ;
param_list    : param (',' param)* ;
param         : value
              | Var ('.' Identifier_)? ':' type_spec
              ;

// Sort
sort_op : Sort_ '(' sort_list ',' query_operator ')' ;
sort_list : '[' sort_expr (',' sort_expr)* ']' ;
sort_expr : Var ':' type_spec sort_spec ;
sort_spec : DescOrder_ | AscOrder_ ;

// Create
create_op     : Create_ '(' (create_rship | create_node) (',' query_operator)? ')' ;
create_node   : '(' Identifier_ ':' Identifier_ property_list? ')';
property_list : '{' property (',' property)* '}' ;
property      : Identifier_ ':' value ;

create_rship : node_var '-' '[' (Identifier_)? ':' Identifier_  property_list? ']' '->' node_var;
node_var     : '(' Var ')' ;

// Lexer
Filter_      : 'Filter' ;
Nodescan_    : 'NodeScan' ;
Indexscan_   : 'IndexScan' ;
Match_       : 'Match' ;
Project_     : 'Project' ;
Limit_       : 'Limit' ;
CrossJoin_   : 'CrossJoin' ;
HashJoin_    : 'HashJoin' ;
LeftOuterJoin_ : 'LeftOuterJoin' ;
Expand_      : 'Expand' ;
ForeachRelationship_ : 'ForeachRelationship' ;
Aggregate_   : 'Aggregate' ;
GroupBy_     : 'GroupBy' ;
Sort_        : 'Sort' ;
Create_      : 'Create' ;
Union_       : 'Union' ;

IntType_     : 'int' ;
Uint64Type_  : 'uint64';
DoubleType_  : 'double' ;
StringType_  : 'string' ;
DateType_    : 'datetime' ;
ResultType_  : 'qresult' ;

Count_       : 'count' ;
Sum_         : 'sum' ;
Avg_         : 'avg' ;
Min_         : 'min' ;
Max_         : 'max' ;
UDF_         : 'udf' ;

InExpandDir_  : 'IN' ;
OutExpandDir_ : 'OUT' ;
FromDir_      : 'FROM' ;
ToDir_        : 'TO' ;
AllDir_       : 'ALL' ;
DescOrder_    : 'DESC' ;
AscOrder_     : 'ASC' ;

INTEGER     : [0-9]+ ;
FLOAT       : '-'? ('0'..'9')+ '.' ('0'..'9')+ ;

Identifier_  : NAME_ ;
Var          : '$'[0-9]+;

NAME_        : [a-zA-Z0-9\-_]+;
STRING_      : '\'' .*? '\'' ;


COLON_    : ':' ;
DOUBLE_COLON : '::' ;
COMMA_    : ',' ;
LPAREN    : '(';
RPAREN    : ')';
LBRACKET  : '[';
RBRACKET  : ']';

OR        :     '||' | 'or';
AND       :     '&&' | 'and';
EQUALS    :    '=' | '==';
NOTEQUALS :    '!=' | '<>';
LT        :    '<';
LTEQ      :    '<=';
GT        :    '>';
GTEQ      :    '>=';
PLUS_     :    '+';
//MINUS     :    '-';
MULT      :    '*';
DIV       :    '/';
MOD       :    '%';
NOT       :    '!' | 'not';

WHITESPACE : [ \t\r\n]+ -> skip ;