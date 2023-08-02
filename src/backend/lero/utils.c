#include "postgres.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <unistd.h>
#include "nodes/pg_list.h"
#include "lero/utils.h"
#include "c.h"
#include "utils/lsyscache.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/cost.h"
#include "nodes/pathnodes.h"
#include "miscadmin.h"
#include "parser/parsetree.h"

#define MSG_SIZE 524288
#define SOCKET_ERR -1
#define SOCKET_SUCC 0

// Connect to the server.
int 
connect_to_server(const char* host, int port) {
  int ret, conn_fd;
  struct sockaddr_in server_addr = { 0 };

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  inet_pton(AF_INET, host, &server_addr.sin_addr);
  conn_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (conn_fd < 0) {
    return conn_fd;
  }
  
  ret = connect(conn_fd, (struct sockaddr*)&server_addr, sizeof(server_addr));
  if (ret == -1) {
    return ret;
  }

  return conn_fd;
}

// Write the entire string to the given socket.
void 
write_all_to_socket(int conn_fd, const char* str) 
{
  size_t str_length;
  ssize_t written, written_total;
  str_length = strlen(str);
  written_total = 0;
  
  while (written_total != str_length) {
    written = write(conn_fd,
                    str + written_total,
                    str_length - written_total);
    written_total += written;
  }
}

char*
concat_str(char* a, char *b)
{
	Assert(a != NULL && b != NULL);
	int size_a = sizeof(a);

	int len_a = strlen(a);
	int len_b = strlen(b);
	char* output = (char *) palloc(len_a + len_b + 1);
	
	memset(output, '\0', sizeof(output));
	strcpy(output, a);
	strcat(output, b);
	return output;
}

char*
send_and_receive_msg(int conn_fd, char* json_str) 
{
	if (conn_fd < 0) {
      elog(WARNING, "Unable to connect to server.");
      return;
    }

	write_all_to_socket(conn_fd, json_str);
	shutdown(conn_fd, SHUT_WR);

	int current_msg_size = MSG_SIZE;
	char *msg = (char *) palloc(current_msg_size * sizeof(char));
	memset(msg, '\0', current_msg_size);
    int ret = 0;
	int offset = 0;

	while ((ret = read(conn_fd, msg + offset, MSG_SIZE - 1)) > 0) {
		offset += ret;
		if (offset >= current_msg_size) {
			int new_msg_size = current_msg_size;
			while(offset >= new_msg_size) {
				new_msg_size *= 2;
			}

			msg = (char *) repalloc(msg, new_msg_size * sizeof(char));
			current_msg_size = new_msg_size;
		}
	}
	msg[offset] = '\0';

	if (SOCKET_ERR == ret) {
		elog(WARNING, "can not read the response from the server.");
	}
	return msg;
}

/**
 * A tricky method to create an unique id of a given query.
 */
char*
get_query_unique_id(const char *queryString)
{
	char* unique_id = (char *) palloc(17);
	memset(unique_id, '\0', 17);
	int query_len = strlen(queryString);
	for (int i = 0; i < 16; i++) 
	{
		char ith = queryString[(rand() % query_len)];
		while (!((ith >= 'a' && ith <='z') || (ith >= 'A' && ith <= 'Z')))
		{
			ith = queryString[(rand() % query_len)];
		}
		unique_id[i] = ith;
	}
	return unique_id;
}

yyjson_doc*
parse_json_str(const char* json)
{
    if (json == NULL || strlen(json) == 0)
    {
        yyjson_mut_doc *json_doc = yyjson_mut_doc_new(NULL);
	    yyjson_mut_val *root = yyjson_mut_obj(json_doc);
        yyjson_mut_doc_set_root(json_doc, root);
	    yyjson_mut_obj_put(root, yyjson_mut_strcpy(json_doc, MSG_TYPE), "error");
        return json_doc;
    }

    return yyjson_read(json, strlen(json), 0);
}

yyjson_mut_val*
double_list_to_json_arr(double l[], int n, yyjson_mut_doc *json_doc)
{
    yyjson_mut_val *arr = yyjson_mut_arr(json_doc);

	for (int i = 0; i < n; i++) {
		yyjson_mut_arr_append(arr, yyjson_mut_real(json_doc, l[i]));
	}
    return arr;
}

yyjson_mut_val*
int_list_to_json_arr(int l[], int n, yyjson_mut_doc *json_doc)
{
    yyjson_mut_val *arr = yyjson_mut_arr(json_doc);

	for (int i = 0; i < n; i++) {
		yyjson_mut_arr_append(arr, yyjson_mut_uint(json_doc, l[i]));
	}
    return arr;
}

yyjson_mut_val*
plan_to_json(PlannedStmt* stmt, Plan *plan, yyjson_mut_doc *json_doc)
{
    yyjson_mut_val *op = yyjson_mut_obj(json_doc);
    yyjson_mut_val *inputs = yyjson_mut_arr(json_doc);
    
    char *op_name;
	char *table_name = NULL;
	char *index_name = NULL;
	char *refname = NULL;
	switch (plan->type)
	{
		case T_SeqScan:
            op_name = "Seq Scan";
			Index seqscan_idx = ((Scan*) plan)->scanrelid;
			table_name = get_rel_name(rt_fetch(seqscan_idx, stmt->rtable)->relid);
			refname = rt_fetch(seqscan_idx, stmt->rtable)->eref->aliasname;
            break;
		case T_IndexScan:
            op_name = "Index Scan";
			Index idxscan_idx = ((Scan*) plan)->scanrelid;
			Index idxscan_index_id = ((IndexScan*) plan)->indexid;

			table_name = get_rel_name(rt_fetch(idxscan_idx, stmt->rtable)->relid);
			index_name = get_rel_name(idxscan_index_id);
			refname = rt_fetch(idxscan_idx, stmt->rtable)->eref->aliasname;
            break;
		case T_IndexOnlyScan:
			op_name = "Index Only Scan";
			Index idxonlyscan_idx = ((Scan*) plan)->scanrelid;
			Index idxonlyscan_index_id = ((IndexOnlyScan*) plan)->indexid;

			table_name = get_rel_name(rt_fetch(idxonlyscan_idx, stmt->rtable)->relid);
			index_name = get_rel_name(idxonlyscan_index_id);
			refname = rt_fetch(idxonlyscan_idx, stmt->rtable)->eref->aliasname;
            break;
		case T_BitmapIndexScan:
			op_name = "Bitmap Index Scan";
			// Index bit_indexscan_idx = ((Scan*) plan)->scanrelid;
			Index bit_indexscan_index_id = ((BitmapIndexScan*) plan)->indexid;

			index_name = get_rel_name(bit_indexscan_index_id);
			// Index bit_idxscan_idx = ((Scan*) plan)->scanrelid;
			// table_name = get_rel_name(rt_fetch(bit_idxscan_idx, stmt->rtable)->relid);
            break;
		case T_BitmapHeapScan:
			op_name = "Bitmap Heap Scan";
			Index bit_heapscan_idx = ((Scan*) plan)->scanrelid;
			table_name = get_rel_name(rt_fetch(bit_heapscan_idx, stmt->rtable)->relid);
			refname = rt_fetch(bit_heapscan_idx, stmt->rtable)->eref->aliasname;
            break;
		case T_HashJoin:
		case T_MergeJoin:
		case T_NestLoop:;
			if (plan->type == T_HashJoin) {
				op_name = "Hash Join";
			} else if (plan->type == T_MergeJoin) {
				op_name = "Merge Join";
			} else {
				op_name = "Nested Loop";
			}

			yyjson_mut_val *inner = plan_to_json(stmt, plan->righttree, json_doc);
			yyjson_mut_val *outer = plan_to_json(stmt, plan->lefttree, json_doc);
			yyjson_mut_arr_append(inputs, outer);
            yyjson_mut_arr_append(inputs, inner);
			break;
		case T_Hash:
            op_name = "Hash";
			yyjson_mut_val *hash_input = plan_to_json(stmt, plan->lefttree, json_doc);
            yyjson_mut_arr_append(inputs, hash_input);
			break;
		case T_Material:
            op_name = "Materialize";
			yyjson_mut_val *mat_input = plan_to_json(stmt, plan->lefttree, json_doc);
            yyjson_mut_arr_append(inputs, mat_input);
			break;
		case T_Sort:
            op_name = "Sort";
			yyjson_mut_val *sort_input = plan_to_json(stmt, plan->lefttree, json_doc);
            yyjson_mut_arr_append(inputs, sort_input);
			break;
		case T_Agg:
            op_name = "Aggregate";
			yyjson_mut_val *agg_input = plan_to_json(stmt, plan->lefttree, json_doc);
            yyjson_mut_arr_append(inputs, agg_input);
			break;
		case T_IncrementalSort:
			op_name = "Incremental Sort";
			yyjson_mut_val *inc_sort_input = plan_to_json(stmt, plan->lefttree, json_doc);
            yyjson_mut_arr_append(inputs, inc_sort_input);
			break;
		case T_Limit:
			op_name = "Limit";
			yyjson_mut_val *limit_input = plan_to_json(stmt, plan->lefttree, json_doc);
            yyjson_mut_arr_append(inputs, limit_input);
			break;
		case T_SampleScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_NamedTuplestoreScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_Append:
		case T_MergeAppend:
		case T_Result:
		case T_ProjectSet:
		case T_Unique:
		case T_Gather:
		case T_Group:
		case T_WindowAgg:
		case T_RecursiveUnion:
		case T_LockRows:
		case T_ModifyTable:
		case T_GatherMerge:
			elog(WARNING, "unrecognized node type: %d",
				 (int) plan->type);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) plan->type);
			break;
	}

    yyjson_mut_obj_put(op, yyjson_mut_strcpy(json_doc, "Node Type"), yyjson_mut_strcpy(json_doc, op_name));
	if (table_name != NULL) {
		yyjson_mut_obj_put(op, yyjson_mut_strcpy(json_doc, "Relation Name"), yyjson_mut_strcpy(json_doc, table_name));
	}
	if (refname != NULL) {
		yyjson_mut_obj_put(op, yyjson_mut_strcpy(json_doc, "Alias"), yyjson_mut_strcpy(json_doc, refname));
	}
	if (index_name != NULL) {
		yyjson_mut_obj_put(op, yyjson_mut_strcpy(json_doc, "Index Name"), yyjson_mut_strcpy(json_doc, index_name));
	}

	if (yyjson_arr_size(inputs)) {
    	yyjson_mut_obj_put(op, yyjson_mut_strcpy(json_doc, "Plans"), inputs);
	}
    yyjson_mut_obj_put(op, yyjson_mut_strcpy(json_doc, "Plan Rows"), yyjson_mut_real(json_doc, plan->plan_rows));
    yyjson_mut_obj_put(op, yyjson_mut_strcpy(json_doc, "Plan Width"), yyjson_mut_sint(json_doc, plan->plan_width));
    yyjson_mut_obj_put(op, yyjson_mut_strcpy(json_doc, "Startup Cost"), yyjson_mut_real(json_doc, plan->startup_cost));
    yyjson_mut_obj_put(op, yyjson_mut_strcpy(json_doc, "Total Cost"), yyjson_mut_real(json_doc, plan->total_cost)); 

    return op;
}

void 
add_join_input_tables(PlannerInfo *root, Path *path, RelatedTable *related_table)
{
	switch (path->pathtype)
	{
		case T_SeqScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:;
			Index table_relid = path->parent->relid;
 			char *table_name = get_rel_name(root->simple_rte_array[table_relid]->relid);
			related_table->tables = lappend(related_table->tables, table_name);
            break;
		case T_HashJoin:
		case T_MergeJoin:
		case T_NestLoop:;
			JoinPath *join_path = (JoinPath *) path;
			Path *inner_path = join_path->innerjoinpath;
			add_join_input_tables(root, inner_path, related_table);
			Path *outer_path = join_path->outerjoinpath;
			add_join_input_tables(root, outer_path, related_table);
			break;
		case T_Material:;
			MaterialPath *material_path = (MaterialPath *) path;
			add_join_input_tables(root, material_path->subpath, related_table);
			break;
		case T_Sort:;
			SortPath *sort_path = (SortPath *) path;
			add_join_input_tables(root, sort_path->subpath, related_table);
			break;
		case T_Agg:;
			AggPath *agg_path = (AggPath *) path;
			add_join_input_tables(root, agg_path->subpath, related_table);
			break;
		case T_SampleScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_NamedTuplestoreScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_Append:
		case T_MergeAppend:
		case T_Result:
		case T_ProjectSet:
		case T_Unique:
		case T_Gather:
		case T_IncrementalSort:
		case T_Group:
		case T_WindowAgg:
		case T_RecursiveUnion:
		case T_LockRows:
		case T_ModifyTable:
		case T_Limit:
		case T_GatherMerge:
			elog(WARNING, "unrecognized node type: %d",
				 (int) path->pathtype);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) path->pathtype);
			break;
	}
}