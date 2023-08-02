#include "yyjson.h"
#include "postgres.h"
#include "optimizer/paths.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "lero_extension.h"

#ifndef LERO_UTILS
#define LERO_UTILS

// msg related
#define MSG_TYPE "msg_type"
#define MSG_INIT "init"
#define MSG_PREDICT "guided_optimization"
#define MSG_QUERY_ID "query_id"

#define MSG_SCORE "latency"
#define MSG_ERROR "error"
#define MSG_FINISH "finish"
#define MSG_END_FLAG "*LERO_END*"

extern int 
connect_to_server(const char* host, int port);

extern void 
write_all_to_socket(int conn_fd, const char *str);

extern char*
concat_str(char* a, char *b);

extern char*
send_and_receive_msg(int conn_fd, char* json_str);

char*
get_query_unique_id(const char *queryString);

extern yyjson_doc*
parse_json_str(const char* json);

extern yyjson_mut_val*
double_list_to_json_arr(double l[], int n, yyjson_mut_doc *json_doc);

extern yyjson_mut_val*
int_list_to_json_arr(int l[], int n, yyjson_mut_doc *json_doc);

extern yyjson_mut_val*
plan_to_json(PlannedStmt* stmt, Plan *plan, yyjson_mut_doc *json_doc);

extern void 
add_join_input_tables(PlannerInfo *root, Path *path, RelatedTable *related_table);

#endif