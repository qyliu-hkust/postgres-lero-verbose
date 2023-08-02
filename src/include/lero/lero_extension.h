#include "postgres.h"
#include "fmgr.h"
#include "optimizer/paths.h"
#include "nodes/plannodes.h"
#include "nodes/pg_list.h"
#include "utils/guc.h"

#ifndef LERO_EXTENSION
#define LERO_EXTENSION
extern bool enable_lero;

extern bool enable_lero_verbose;

extern int lero_server_port;

extern char *lero_server_host;

typedef struct LeroPlan {
	double *card;

	PlannedStmt* plan;

	double latency;

	double act_total_time;
} LeroPlan;

typedef struct RelatedTable {
    List *tables;
} RelatedTable;

extern void lero_pgsysml_set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
						   RelOptInfo *outer_rel,
						   RelOptInfo *inner_rel,
						   SpecialJoinInfo *sjinfo,
						   List *restrictlist);

extern PlannedStmt* lero_pgsysml_hook_planner(Query *parse, const char *queryString,
                                int cursorOptions,
                                ParamListInfo boundParams);
#endif