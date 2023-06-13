#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "crossdb.h"

typedef struct {
	uint32_t 			prefix;
	uint8_t 			mask;
	uint32_t			nexthop;
	uint8_t 			metric;
	char				intf[16];
	time_t				birth;
	uint32_t			flags;
} route_t;

#undef	CROSS_STRUCT_NAME
#define	CROSS_STRUCT_NAME	route_t
cross_field_t 	route_schema[] = {
	CROSS_FIELD (prefix,	UINT,	IPv4, 0),
	CROSS_FIELD (mask, 		UINT,	DFT,  0),
	CROSS_FIELD (nexthop,	UINT,	IPv4, 0),
	CROSS_FIELD (metric, 	UINT,	DFT,  0),
	CROSS_FIELD (intf,		CHAR,	DFT,  0),
	CROSS_FIELD (birth, 	UINT,	TS,   0),
	CROSS_FIELD (flags, 	UINT,	HEX,  0),
	CROSS_END (route_t)
};

int main ()
{
	cross_db_h 		hDb;
	cross_tbl_h 	hRtTbl;
	cross_ret 		ret;
	route_t 		route;	
	cross_rowid 	count;

	#define CHECK(ret,str)		if (ret < 0) {	printf (str": %s\n", cross_errMsg(ret)); return -1; }
	#define EXPECT(val,exp,str)	if (val != exp) {	printf ("%s: %d != %d\n", str, val, exp); return -1; } \
								else  { printf ("%s: %d\n", str, val); }

	#define IP4ADDR(a,b,c,d)		(uint32_t)((a)<<24|(b)<<16|(c)<<8|(d))
	#define IP4STR(ip)				ip>>24,(ip>>16)&0xff,(ip>>8)&0xff,ip&0xff
	#define DUMP_ROUTE(str, route)	printf (str"%d.%d.%d.%d/%d->%d.%d.%d.%d intf: %s metric: %d flags: 0x%x\n", 	\
							IP4STR(route.prefix), route.mask, IP4STR(route.nexthop), route.intf, route.metric, route.flags);

	/*
	 * Create DB
	 */
	printf ("\n============ Create DB & Table ============\n");

	printf ("  Create database : tutorial\n");

	ret = cross_dbCreate (&hDb, "db_data/tutorial", 0);
	CHECK (ret, "Failed to create DB: tutorial");

	printf ("  Create table: route (PrimaryKey: prefix,mask)\n");
	ret = cross_dbTblCreate (hDb, &hRtTbl, "route", route_schema, "prefix,mask", 0);
	CHECK (ret, "Failed to create table: route");

	printf ("  Create index on nexthop: idx_nexthop\n");
	ret = cross_dbIdxCreate (hRtTbl, "idx_nexthop", "nexthop", 0);
	CHECK (ret, "Failed to create index: idx_nexthop");


	// Delete all rows
	cross_dbDeleteRows (hRtTbl, NULL, NULL, 0);


	/*
	 * Insert Rows
	 */
	printf ("\n============ Insert Rows ============\n");

	printf ("  Insert route 192.168.1.0/24->192.168.1.254\n");
	route.prefix	= IP4ADDR(192,168,1,0);
	route.mask		= 24;	
	route.nexthop	= IP4ADDR(192,168,1,254);
	route.metric	= 1;
	route.flags		= 0;
	strcpy (route.intf, "eth1");
	route.birth		= time (NULL);
	ret = cross_dbInsertRow (hRtTbl, &route, 0); 
	CHECK (ret, "Failed to insert route 192.168.1.0/24");

	printf ("  Insert route 10.1.1.0/24->10.1.2.254\n");
	route.prefix	= IP4ADDR(10,1,1,0);
	route.mask		= 24;	
	route.nexthop	= IP4ADDR(10,1,2,254);
	route.metric	= 2;
	route.flags		= 0;
	strcpy (route.intf, "eth2");
	route.birth		= time (NULL);
	ret = cross_dbInsertRow (hRtTbl, &route, 0); 
	CHECK (ret, "Failed to insert route 10.1.1.0/24");

	printf ("  Use Replace to insert route 10.1.2.0/24->10.1.2.254\n");
	route.prefix	= IP4ADDR(10,1,2,0);
	route.mask		= 24;	
	route.nexthop	= IP4ADDR(10,1,2,254);
	route.metric	= 1;
	route.flags		= 0;
	strcpy (route.intf, "eth2");
	route.birth		= time (NULL);
	ret = cross_dbReplaceRow (hRtTbl, &route, 0); 
	CHECK (ret, "Failed to replace route 10.1.2.0/24");


	/*
	 * Query Rows
	 */

	printf ("\n============ Query Rows ============\n");

	// Get all rows count
	count = cross_dbGetRowsCount (hRtTbl, NULL, NULL, 0);
	EXPECT (count, 3, "  Total routes");

	// Get rows count where nexthop=10.1.2.254
	route.nexthop	= IP4ADDR(10,1,2,254);
	count = cross_dbGetRowsCount (hRtTbl, "nexthop", &route, 0);
	EXPECT (count, 2, "  Nexthop=10.1.2.254 routes");

	// Get single route 192.168.1.0/24 by PrimaryKey
	memset (&route, 0, sizeof(route));
	route.prefix	= IP4ADDR(192,168,1,0);
	route.mask		= 24;	
	ret = cross_dbGetRowByPK (hRtTbl, &route, &route, 0); 
	CHECK (ret, "Failed to get route 192.168.1.0/24 by PrimaryKey");
	DUMP_ROUTE ("  Get single route: ", route);

	// Get one row where nexthop=10.1.2.254
	memset (&route, 0, sizeof(route));	
	route.nexthop	= IP4ADDR(10,1,2,254);
	ret = cross_dbGetOneRow (hRtTbl, "nexthop", &route, &route, 0);
	CHECK (ret, "Failed to get one route where nexthop=10.1.2.254");
	DUMP_ROUTE ("  Get one route where nexthop=10.1.2.254: ", route);

	// Get one row where nexthop!=10.1.2.254
	memset (&route, 0, sizeof(route));	
	route.nexthop	= IP4ADDR(10,1,2,254);
	ret = cross_dbGetOneRow (hRtTbl, "nexthop!=", &route, &route, 0);
	CHECK (ret, "Failed to get one route where nexthop!=10.1.2.254");
	DUMP_ROUTE ("  Get one route where nexthop!=10.1.2.254: ", route);


	/*
	 * Update Rows
	 */

	printf ("\n============ Update Rows ============\n");

	printf ("  Update single route 192.168.1.0/24 by Primary Key: set flags 0->1 metric 1->3\n");
	route.prefix	= IP4ADDR(192,168,1,0);
	route.mask		= 24;	
	route.metric	= 3;
	route.flags		= 1;
	ret = cross_dbUpdRowByPK (hRtTbl, &route, "flags,metric", &route, 0); 
	CHECK (ret, "Failed to update route 192.168.1.0/24 by Primary Key");
	// verify
	route.flags 	= 0;
	ret = cross_dbGetRowByPK (hRtTbl, &route, &route, 0);
	EXPECT (route.flags, 1, "    Update flags");
	EXPECT (route.metric, 3,   "    Update metric");
	DUMP_ROUTE ("    Get single route: ", route);

	printf ("  Use Replace to Update single route 192.168.1.0/24 by Primary Key: set flags 1->2\n");
	route.prefix	= IP4ADDR(192,168,1,0);
	route.mask		= 24;	
	route.flags		= 2;
	ret = cross_dbReplaceRow (hRtTbl, &route, 0); 
	CHECK (ret, "Failed to replace route 192.168.1.0/24");
	// verify
	route.flags 	= 0;
	ret = cross_dbGetRowByPK (hRtTbl, &route, &route, 0);
	EXPECT (route.flags, 2, "    Update flags");
	DUMP_ROUTE ("    Get single route: ", route);

	// Update routes where nexthop=10.1.2.254: set flags 0->3
	route.nexthop	= IP4ADDR(10,1,2,254);
	route.flags		= 3;
	count = cross_dbUpdateRows (hRtTbl, "nexthop", &route, "flags", &route, 0);
	EXPECT (count, 2, "  Update nexthop=10.1.2.254 set flags=3 rows");

	// Update All routes set flags 0->3
	route.nexthop	= IP4ADDR(10,1,2,254);
	route.flags		= 4;
	count = cross_dbUpdateRows (hRtTbl, NULL, NULL, "flags", &route, 0);
	EXPECT (count, 3, "  Update all routes set flags=4 rows");


	/*
	 * Cursor Query
	 */

	printf ("\n============ Cursor Query ============\n");

	// Use cursor to get all routes
	cross_cursor_h hCursor;
	count = cross_dbQueryRows (hRtTbl, &hCursor, NULL, &route, 0);
	EXPECT (count, 3, "  Query All routes");
	while (CROSS_OK == cross_cursorGetNextRow (hCursor, &route, 0)) {
		DUMP_ROUTE ("    route: ", route);
	}

	// Reuse cursor to get routes where nexthop=10.1.2.254
	route.nexthop	= IP4ADDR(10,1,2,254);
	count = cross_dbQueryRows (hRtTbl, &hCursor, "nexthop", &route, CROSS_REUSE);
	EXPECT (count, 2, "  Query nexthop=10.1.2.254 routes");
	while (CROSS_OK == cross_cursorGetNextRow (hCursor, &route, 0)) {
		DUMP_ROUTE ("    route: ", route);
	}

	// Reuse cursor to get routes where nexthop!=10.1.2.254
	route.nexthop	= IP4ADDR(10,1,2,254);
	count = cross_dbQueryRows (hRtTbl, &hCursor, "nexthop!=", &route, CROSS_REUSE);
	EXPECT (count, 1, "  Query nexthop!=10.1.2.254 routes");
	while (CROSS_OK == cross_cursorGetNextRow (hCursor, &route, 0)) {
		DUMP_ROUTE ("    route: ", route);
	}

	cross_cursorClose (hCursor, 0);


	/*
	 * Transaction
	 */

	printf ("\n============ Transaction ============\n");
	
	printf ("  Commit\n");
	ret = cross_dbTransBegin (hDb, 0);
	CHECK (ret, "Failed to begin transaction");
	printf ("    Update single route 192.168.1.0/24 by Primary Key: set flags 0->5\n");
	route.prefix	= IP4ADDR(192,168,1,0);
	route.mask		= 24;	
	route.flags		= 5;
	ret = cross_dbUpdRowByPK (hRtTbl, &route, "flags", &route, 0); 
	CHECK (ret, "Failed to update route 192.168.1.0/24 by Primary Key");
	ret = cross_dbTransCommit (hDb, 0);
	CHECK (ret, "Failed to commit transaction");
	// verify
	route.flags 	= 0;
	ret = cross_dbGetRowByPK (hRtTbl, &route, &route, 0);
	EXPECT (route.flags, 5, "    Update flags");
	DUMP_ROUTE ("    Get single route: ", route);

	printf ("  Rollback\n");
	ret = cross_dbTransBegin (hDb, 0);
	CHECK (ret, "Failed to begin transaction");
	printf ("    Update single route 192.168.1.0/24 by Primary Key: set flags 0->6\n");
	route.prefix	= IP4ADDR(192,168,1,0);
	route.mask		= 24;	
	route.flags		= 6;
	ret = cross_dbUpdRowByPK (hRtTbl, &route, "flags", &route, 0); 
	CHECK (ret, "Failed to update route 192.168.1.0/24 by Primary Key");
	ret = cross_dbTransRollback (hDb, 0);
	CHECK (ret, "Failed to commit transaction");
	// verify
	route.flags 	= 0;
	ret = cross_dbGetRowByPK (hRtTbl, &route, &route, 0);
	EXPECT (route.flags, 5, "    Rollback to orignal flags");
	DUMP_ROUTE ("    Get single route: ", route);


	/*
	 * Delete Rows
	 */

	printf ("\n============ Delete Rows ============\n");

	printf ("  Delete single route 192.168.1.0/24 by Primary Key\n");
	route.prefix	= IP4ADDR(192,168,1,0);
	route.mask		= 24;	
	ret = cross_dbDelRowByPK (hRtTbl, &route, 0); 
	CHECK (ret, "Failed to delete route 192.168.1.0/24 by Primary Key");
	// verify
	count = cross_dbGetRowsCount (hRtTbl, NULL, NULL, 0);
	EXPECT (count, 2, "    Total routes");

	// Delete routes where nexthop=10.1.2.254
	route.nexthop	= IP4ADDR(10,1,2,254);
	count = cross_dbDeleteRows (hRtTbl, "nexthop", &route, 0);
	EXPECT (count, 2, "  Delete nexthop=10.1.2.254 rows");
	// verify
	count = cross_dbGetRowsCount (hRtTbl, NULL, NULL, 0);
	EXPECT (count, 0, "    Total routes");


	/*
	 * Close DB
	 */
	printf ("\n============ Close DB ============\n");
	cross_dbClose (hDb, 0);

	return 0;
}

