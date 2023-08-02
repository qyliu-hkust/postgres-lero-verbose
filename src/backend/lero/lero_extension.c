#include "lero/lero_extension.h"
#include "miscadmin.h"
#include "optimizer/appendinfo.h"
#include "optimizer/joininfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "optimizer/paths.h"
#include "partitioning/partbounds.h"
#include "nodes/bitmapset.h"
#include "utils/memutils.h"
#include "lero/utils.h"
#include "lero/yyjson.h"
#include "commands/explain.h"
#include <string.h>
#include <stdlib.h>

#define PLAN_MAX_SAMPLES 1024

#define CARD_MAX_NUM 25000

bool enable_lero = false;

// if true, lero will execute every candidate plan for better debugging
bool enable_lero_verbose = false;

// lero server configuration
int lero_server_port = 14567;
char *lero_server_host = "localhost";

// the number of join cardinalities to be calculated in this query
int join_card_num = -1;
// the original join cardinalities without any reduction
double original_card_list[CARD_MAX_NUM] = {-1.0};
// indicates whether to record the original join cardinalities or read the new join cardinalities after zooming
bool record_original_card_phase = false;
// the new join cardinalities after zooming given by lero
double lero_card_list[CARD_MAX_NUM] = {-1.0};
// the number of join cardinalities given by lero
int max_lero_join_card_idx = -1;
// recored the names of the input tables involved in each join
RelatedTable *join_input_tables[CARD_MAX_NUM] = {NULL};

int cur_card_idx = 0;

char* query_unique_id = NULL;


static
LeroPlan *get_lero_plan(int i, Query *parse, const char *queryString,
					  int cursorOptions,
					  ParamListInfo boundParams, int *early_stop);
static 
double predict_plan_score(yyjson_mut_doc *json_doc, yyjson_mut_val *json_root, int *early_stop);

static 
void send_default_rows(const char *queryString);

static 
void get_join_card_list();

static 
void remove_opt_state();

void lero_pgsysml_set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
											RelOptInfo *outer_rel,
											RelOptInfo *inner_rel,
											SpecialJoinInfo *sjinfo,
											List *restrictlist)
{
	static double rows;
	if (record_original_card_phase)
	{
		rows = rel->rows;
		original_card_list[join_card_num] = rows;

		RelatedTable *related_table = (RelatedTable *) palloc(sizeof(RelatedTable));
		related_table->tables = NIL;
		add_join_input_tables(root, outer_rel->cheapest_total_path, related_table);
		add_join_input_tables(root, inner_rel->cheapest_total_path, related_table);
		join_input_tables[join_card_num] = related_table;
		join_card_num += 1;
	}
	else
	{
		if (cur_card_idx <= max_lero_join_card_idx) {
			rows = lero_card_list[cur_card_idx];
		} else {
			rows = rel->rows;
		}
		cur_card_idx += 1;
	}
	rel->rows = rows;
}

PlannedStmt *lero_pgsysml_hook_planner(Query *parse, const char *queryString,
									  int cursorOptions,
									  ParamListInfo boundParams)
{
	if (!enable_lero)
	{
		return standard_planner(parse, queryString, cursorOptions, boundParams);
	}

	query_unique_id = get_query_unique_id(queryString);

	LeroPlan *plan_for_card[PLAN_MAX_SAMPLES];
	Query *query_copy;
	int conn_fd;
	record_original_card_phase = false;

	LeroPlan* best = NULL;
	double best_latency;
	int best_idx = 0;
	join_card_num = 0;
	int plan_num = PLAN_MAX_SAMPLES;
	int early_stop = 0;
	for (int i = 0; i < PLAN_MAX_SAMPLES; i++)
	{
		// Plan the query for this card list.
		query_copy = copyObject(parse);
		LeroPlan* p = get_lero_plan(i, query_copy,
										queryString, cursorOptions, boundParams, &early_stop);
		if (best == NULL || p->latency < best_latency) {
			best = p;
			best_latency = p->latency;
			best_idx = i;
		}
		plan_for_card[i] = p;

		if (early_stop) {
			plan_num = i + 1;
			break;
		}
	}

	for (int i = 0; i < plan_num; i++) {
		elog(WARNING, "%d-th plan's prediction score is %f true time is %f", i, plan_for_card[i]->latency, plan_for_card[i]->act_total_time);
	}

	remove_opt_state();
	pfree(query_unique_id);
	elog(WARNING, "best plan is %d", best_idx);
	return best->plan;
}

double get_double(const char *str)
{
    /* First skip non-digit characters */
    /* Special case to handle negative numbers and the `+` sign */
    while (*str && !(isdigit(*str) || ((*str == '-' || *str == '+') && isdigit(*(str + 1)))))
        str++;

    /* The parse to a double */
    return strtod(str, NULL);
}

static
LeroPlan *get_lero_plan(int i, Query *parse, const char *queryString,
					  int cursorOptions,
					  ParamListInfo boundParams, int* early_stop)
{
	// do not change the cardinality list for the first query planning
	// and send the default cardinality list to the server
	record_original_card_phase = i == 0;
	cur_card_idx = 0;
	LeroPlan *p = (LeroPlan *)palloc(sizeof(LeroPlan));
	elog(WARNING, "Query string:%s", queryString);

	if (!record_original_card_phase) {
		get_join_card_list();
	}

	PlannedStmt *plan = standard_planner(parse, queryString, cursorOptions, boundParams);
	p->plan = plan;

	if (record_original_card_phase)
	{
		send_default_rows(queryString);
	}

	if (enable_lero_verbose)
	{
		instr_time	planduration;
		ExplainState *es = NewExplainState();
		es->analyze = true;
		es->timing = true;
		es->buffers = false;
		es->summary = true;
		ExplainFormat f = EXPLAIN_FORMAT_TEXT;
		es->format = f;
		QueryEnvironment *queryEnv = create_queryEnv();
		ExplainOnePlan(plan, NULL, es, queryString, boundParams, queryEnv, &planduration, NULL);
		char *explain_str = es->str->data;
		char *token = strtok(explain_str, "\n");
		while (token != NULL)
		{
			if (strstr(token, "Execution Time") != NULL)
				break;
			token = strtok(NULL, "\n");
		}
		double act_time = get_double(token);
		elog(WARNING, "Explain Execution Time: %f ms", act_time);
		p->act_total_time = act_time;
	}

	yyjson_mut_doc *json_doc = yyjson_mut_doc_new(NULL);
	yyjson_mut_val *root = yyjson_mut_obj(json_doc);	
	yyjson_mut_doc_set_root(json_doc, root);
	yyjson_mut_val *plan_json = plan_to_json(plan, plan->planTree, json_doc);
	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, "Execution Time"), yyjson_mut_real(json_doc, p->act_total_time));
	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, "Plan"), plan_json);
	p->latency = predict_plan_score(json_doc, root, early_stop);
	yyjson_mut_doc_free(json_doc);
	return p;
}

static 
void send_default_rows(const char *queryString)
{
	int conn_fd = connect_to_server(lero_server_host, lero_server_port);
	if (conn_fd == -1)
	{
		elog(WARNING, "%s:%d", lero_server_host, lero_server_port);
		elog(ERROR, "Unable to connect to Lero server.");
		return;
	}

	yyjson_mut_doc *json_doc = yyjson_mut_doc_new(NULL);
	yyjson_mut_val *root = yyjson_mut_obj(json_doc);
	yyjson_mut_doc_set_root(json_doc, root);

	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, MSG_TYPE), yyjson_mut_strcpy(json_doc, MSG_INIT));

	yyjson_mut_val *row_arr = yyjson_mut_arr(json_doc);
	for (int i = 0; i < join_card_num; i++) {
		yyjson_mut_arr_append(row_arr, yyjson_mut_real(json_doc, original_card_list[i]));
	}

	yyjson_mut_val *table_arr = yyjson_mut_arr(json_doc);
	for (int i = 0; i < join_card_num; i++) {
		yyjson_mut_val *arr = yyjson_mut_arr(json_doc);
		RelatedTable *related_table = join_input_tables[i];
		ListCell   *lc;
		foreach(lc, related_table->tables) {
			char* table_name = (char *) lfirst(lc);
			yyjson_mut_arr_append(arr, yyjson_mut_strcpy(json_doc, table_name));
		}
		list_free(related_table->tables);
		pfree(related_table);

		yyjson_mut_arr_append(table_arr, arr);
	}

	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, "rows_array"), row_arr);
	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, "table_array"), table_arr);
	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, "max_samples"),
					   yyjson_mut_uint(json_doc, PLAN_MAX_SAMPLES));
	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, MSG_QUERY_ID), 
					   yyjson_mut_strcpy(json_doc, query_unique_id));		   

	char *json = yyjson_mut_write(json_doc, YYJSON_WRITE_PRETTY, NULL);
	char *send_json = concat_str(json, MSG_END_FLAG);
	if (json) {
		free(json);
	}

	// check whether Lero is initialized
	char *msg = send_and_receive_msg(conn_fd, send_json);
	yyjson_doc *msg_doc = parse_json_str(msg);
	pfree(msg);

	yyjson_val *msg_json_obj = yyjson_doc_get_root(msg_doc);
	yyjson_val *msg_type = yyjson_obj_get(msg_json_obj, MSG_TYPE);
	char *msg_char = yyjson_get_str(msg_type);
	if (strcmp(msg_char, MSG_ERROR) == 0)
	{
		yyjson_doc_free(msg_doc);
		elog(ERROR, "fail to init Lero");
		return;
	}

	if (send_json) {
		pfree((void *) send_json);
	}
	yyjson_doc_free(msg_doc);
	yyjson_mut_doc_free(json_doc);
	close(conn_fd);
}

static 
double predict_plan_score(yyjson_mut_doc *json_doc, yyjson_mut_val *json_root, int *early_stop) {
	yyjson_mut_obj_put(json_root, yyjson_mut_strcpy(json_doc, MSG_TYPE), yyjson_mut_strcpy(json_doc, MSG_PREDICT));
	yyjson_mut_obj_put(json_root, yyjson_mut_strcpy(json_doc, MSG_QUERY_ID), yyjson_mut_strcpy(json_doc, query_unique_id));
	char *json = yyjson_mut_write(json_doc, YYJSON_WRITE_PRETTY, NULL);
	json = concat_str(json, MSG_END_FLAG);

	int conn_fd = connect_to_server(lero_server_host, lero_server_port);
	if (conn_fd == -1)
	{
		elog(ERROR, "Unable to connect to Lero server.");
	}

	char *msg = send_and_receive_msg(conn_fd, json);
	yyjson_doc *msg_doc = parse_json_str(msg);
	pfree(msg);

	yyjson_val *msg_json_obj = yyjson_doc_get_root(msg_doc);
	yyjson_val *msg_type = yyjson_obj_get(msg_json_obj, MSG_TYPE);
	char *msg_char = yyjson_get_str(msg_type);

	if (strcmp(msg_char, MSG_ERROR) == 0)
	{
		yyjson_doc_free(msg_doc);
		close(conn_fd);
		elog(ERROR, "fail to get score from Lero");
	} else {
		*early_stop = yyjson_get_int(yyjson_obj_get(msg_json_obj, MSG_FINISH));
		double score = yyjson_get_real(yyjson_obj_get(msg_json_obj, MSG_SCORE));
		yyjson_doc_free(msg_doc);
		close(conn_fd);
		return score;
	}
}

static 
void get_join_card_list() {
	yyjson_mut_doc *json_doc = yyjson_mut_doc_new(NULL);
	yyjson_mut_val *root = yyjson_mut_obj(json_doc);
	yyjson_mut_doc_set_root(json_doc, root);
	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, MSG_TYPE), yyjson_mut_strcpy(json_doc, "join_card"));
	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, MSG_QUERY_ID), yyjson_mut_strcpy(json_doc, query_unique_id));
	
	char *json = yyjson_mut_write(json_doc, YYJSON_WRITE_PRETTY, NULL);
	char *send_json = concat_str(json, MSG_END_FLAG);
	if (json) {
		free(json);
	}

	int conn_fd = connect_to_server(lero_server_host, lero_server_port);
	if (conn_fd == -1)
	{
		elog(ERROR, "Unable to connect to Lero server.");
	}
	char *msg = send_and_receive_msg(conn_fd, send_json);
	yyjson_doc *msg_doc = parse_json_str(msg);
	
	close(conn_fd);
	yyjson_mut_doc_free(json_doc);
	pfree(msg);
	pfree(send_json);
	
	yyjson_val *msg_json_obj = yyjson_doc_get_root(msg_doc);
	yyjson_val *msg_type = yyjson_obj_get(msg_json_obj, MSG_TYPE);
	char *msg_char = yyjson_get_str(msg_type);

	if (strcmp(msg_char, MSG_ERROR) == 0)
	{
		yyjson_doc_free(msg_doc);
		elog(ERROR, "fail to get latency from Lero");
	} else {
		yyjson_val *joinrel_card_list_val = yyjson_obj_get(msg_json_obj, "join_card");		
		yyjson_val *val;
		yyjson_arr_iter iter;
		yyjson_arr_iter_init(joinrel_card_list_val, &iter);
		int i = -1;
		while ((val = yyjson_arr_iter_next(&iter))) {
			i++;
			double real = yyjson_get_real(val);
			printf("%f", real);
			lero_card_list[i] = real;
		}

		max_lero_join_card_idx = i;
		yyjson_doc_free(msg_doc);
	}
}

static 
void remove_opt_state() {
	yyjson_mut_doc *json_doc = yyjson_mut_doc_new(NULL);
	yyjson_mut_val *root = yyjson_mut_obj(json_doc);
	yyjson_mut_doc_set_root(json_doc, root);
	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, MSG_TYPE), yyjson_mut_strcpy(json_doc, "remove_state"));
	yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, MSG_QUERY_ID), yyjson_mut_strcpy(json_doc, query_unique_id));
	
	char *json = yyjson_mut_write(json_doc, YYJSON_WRITE_PRETTY, NULL);
	char *send_json = concat_str(json, MSG_END_FLAG);
	if (json) {
		free(json);
	}

	int conn_fd = connect_to_server(lero_server_host, lero_server_port);
	if (conn_fd == -1)
	{
		elog(ERROR, "Unable to connect to Lero server.");
	}
	char *msg = send_and_receive_msg(conn_fd, send_json);
	yyjson_doc *msg_doc = parse_json_str(msg);
	
	close(conn_fd);
	yyjson_mut_doc_free(json_doc);
	pfree(msg);
	pfree(send_json);
	
	yyjson_val *msg_json_obj = yyjson_doc_get_root(msg_doc);
	yyjson_val *msg_type = yyjson_obj_get(msg_json_obj, MSG_TYPE);
	char *msg_char = yyjson_get_str(msg_type);

	if (strcmp(msg_char, MSG_ERROR) == 0)
	{
		elog(WARNING, "fail to remove state");
	}
	yyjson_doc_free(msg_doc);
}