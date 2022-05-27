class GenericChecks:
    def __init__(self):
        self.sqls = []

    def trending(self, dq_name, threshold, stop_on_failure, columns, is_trial, description, vars,
        compare_type=None, threshold_min=None):
        if len(columns) == 0:
            raise Exception('TRENDING check requires [columns] to be specified')

        # If there's a min threshold specified, execute a different logic
        # and no more for up and down
        if threshold_min is not None:
            compare_logic = 'abs(t.dq_tgt_value - s.dq_src_value)/(s.dq_src_value*1.0) between {tm} and {t}'.format(
                t=threshold,
                tm=threshold_min,
                )
            threshold = '{tm},{t}'.format(
                t=threshold,
                tm=threshold_min,
                )
            trending_type = 'range'
        else:
            # Feature for checking up and down trending
            if '+' in str(threshold):
                compare_logic = '(t.dq_tgt_value - s.dq_src_value)/(s.dq_src_value*1.0) between 0 and {t}'.format(
                    t=threshold.replace('+', '')
                    )
                trending_type = 'upward'

            elif '-' in str(threshold):
                compare_logic = '(t.dq_tgt_value - s.dq_src_value)/(s.dq_src_value*1.0) between {t} and 0'.format(
                    t=threshold
                    )
                trending_type = 'downward'           
            else:
                compare_logic = 'abs(t.dq_tgt_value - s.dq_src_value)/(s.dq_src_value*1.0) <= {t}'.format(
                    t=threshold
                    )
                trending_type = 'absolute'

        # Set comparison type (default to last previous run)
        desc = description.replace("'", "''")
        if compare_type == 'day':
            dq_date_range = "dq_run_hour::date = current_date - interval '1 day'"
            desc += " (day-over-day)"
        elif compare_type == 'week':
            dq_date_range = "dq_run_hour::date = current_date - interval '7 day'"
            desc += " (week-over-week)"
        elif compare_type == 'month':
            dq_date_range = "dq_run_hour::date = current_date - interval '1 month'"
            desc += " (month-over-month)"
        elif compare_type == 'year':
            dq_date_range = "dq_run_hour::date = current_date - interval '1 year'"
            desc += " (year-over-year)"
        else:
            dq_date_range = "1=1"

        for column in columns:
            self.sqls.append("""
                -- Trending type: {trending_type}
                {insert}
                SELECT
                    '{dq_run_hour}' AS dq_run_hour
                    ,'{database_name}' AS database_name
                    ,'{schema_name}' AS schema_name
                    ,'{table_name}' AS table_name
                    ,'{table_filter}' AS table_filter
                    ,'trending' AS dq_name
                    ,'{column_desc}' AS dq_column
                    ,'{desc}' AS dq_description
                    ,CAST(t.dq_tgt_value AS VARCHAR) AS dq_tgt_value
                    ,CAST(s.dq_src_value AS VARCHAR) AS dq_src_value
                    ,'{threshold}' AS dq_threshold
                    ,CASE WHEN s.dq_src_value IS NULL THEN true
                        WHEN s.dq_src_value = 0 THEN false
                        WHEN t.dq_tgt_value = 0 THEN false
                        WHEN {logic} THEN true
                        ELSE false END AS is_pass
                    ,{stop} AS stop_on_failure
                    ,false AS is_dq_custom
                    ,{dq_key} AS dq_key
                    ,CURRENT_TIMESTAMP AS dq_start_tstamp
                    ,NULL AS dq_end_tstamp
                    ,'{db_username}' AS db_username
                    ,'{unix_username}' AS unix_username
                    ,'{env}' AS env
                    ,{trial} AS is_trial
                FROM
                    (
                        SELECT {column} AS dq_tgt_value
                        FROM {target_table}
                        WHERE {target_filter}
                    ) t LEFT OUTER JOIN
                    (
                        SELECT table_name, CAST(dq_tgt_value AS FLOAT) AS dq_src_value
                        FROM {dq_table_prod}
                        WHERE database_name = '{database_name}' AND schema_name = '{schema_name}' AND table_name = '{table_name}'
                          AND dq_name = 'trending' AND dq_column = '{column_desc}'
                          AND env = '{env}'
                          AND {dq_date_range}
                        ORDER BY dq_run_hour DESC LIMIT 1
                    ) s
                    ON (1=1)
                ;
            """.format(
                trending_type=trending_type,
                insert=vars['insert_sql'],
                dq_run_hour=vars['dq_run_hour'],
                database_name=vars['target_database_name'],
                schema_name=vars['target_schema_name'],
                table_name=vars['target_table_name'],
                table_filter=vars['target_filter'].replace("1=1","").replace("'", "''"),
                column_desc=column.replace("'", "''"),
                column=column,
                desc=desc,
                target_table=vars['target_table'],
                target_filter=vars['target_filter'],
                threshold=threshold,
                logic=compare_logic,
                stop=stop_on_failure,
                dq_key=vars['dq_key'],
                dq_table=vars['dq_table'],
                dq_table_prod=vars['dq_table_prod'],
                db_username=vars['db_username'],
                unix_username=vars['unix_username'],
                env=vars['env'],
                trial=is_trial,
                dq_date_range=dq_date_range,
                )
            )

        return self.sqls

    def compare_to_source(self, dq_name, threshold, stop_on_failure, columns, is_trial, description, vars):
        self.sqls.append("""
            {insert}
            SELECT
                '{dq_run_hour}' AS dq_run_hour
                ,'{database_name}' AS database_name
                ,'{schema_name}' AS schema_name
                ,'{table_name}' AS table_name
                ,'{table_filter}' AS table_filter
                ,'compare_to_source' AS dq_name
                ,'count(*)' AS dq_column
                ,'{desc}' AS dq_description
                ,CAST(t.cnt AS VARCHAR) AS dq_tgt_value
                ,CAST(s.cnt AS VARCHAR) AS dq_src_value
                ,'{threshold}' AS dq_threshold
                ,CASE WHEN s.cnt IS NULL THEN false
                    WHEN s.cnt = 0 THEN false
                    WHEN t.cnt = 0 THEN false
                    WHEN abs(t.cnt - s.cnt)/(s.cnt*1.0) <= {threshold} THEN true
                    ELSE false END AS is_pass
                ,{stop} AS stop_on_failure
                ,false AS is_dq_custom
                ,{dq_key} AS dq_key
                ,CURRENT_TIMESTAMP AS dq_start_tstamp
                ,NULL AS dq_end_tstamp
                ,'{db_username}' AS db_username
                ,'{unix_username}' AS unix_username
                ,'{env}' AS env
                ,{trial} AS is_trial
            FROM
                (SELECT count(*) cnt FROM {target_table} WHERE {target_filter}) t,
                (SELECT count(*) cnt FROM {source_table} WHERE {source_filter}) s
            ;
        """.format(
            insert=vars['insert_sql'],
            dq_run_hour=vars['dq_run_hour'],
            database_name=vars['target_database_name'],
            schema_name=vars['target_schema_name'],
            table_name=vars['target_table_name'],
            table_filter=vars['target_filter'].replace("1=1","").replace("'", "''"),
            desc=description.replace("'", "''"),
            target_table=vars['target_table'],
            target_filter=vars['target_filter'],
            source_table=vars['source_table'],
            source_filter=vars['source_filter'],
            threshold=threshold,
            stop=stop_on_failure,
            dq_key=vars['dq_key'],
            db_username=vars['db_username'],
            unix_username=vars['unix_username'],
            env=vars['env'],
            trial=is_trial,
            )
        )

        return self.sqls

    def empty_null(self, dq_name, threshold, stop_on_failure, columns, is_trial, description, vars):
        # Default threshold to 0
        threshold = 0

        if len(columns) == 0:
            raise Exception('EMPTY_NULL check requires [columns] to be specified')
        # This is for a single query for all columns
        select_list = []
        with_clause = """
                WITH subq AS
                (SELECT count(*) total_cnt
        """
        i = 1
        # Keep dq_columns as 1 element since we have a single query to do it for all columns
        # and expand later when checking for pass/fail result
        for column in columns:
            with_clause += """
                    ,'{column1}'::VARCHAR AS col{i}_nm
                    ,sum(case when length(CAST({column2} AS VARCHAR)) = 0
                        or {column2} is null then 1 else 0 end) AS col{i}_cnt
            """.format(column1=column.replace("'", "''"), column2=column, i=i)
            select_list.append("""
                SELECT total_cnt, col{i}_nm AS dq_column, col{i}_cnt AS empty_null_cnt FROM subq
            """.format(i=i))
            i += 1
        with_clause += """
                FROM {target_table}
                WHERE {target_filter})
        """.format(
            target_table=vars['target_table'],
            target_filter=vars['target_filter'],
            )
        with_clause += ' UNION ALL '.join(select_list)
        self.sqls.append("""
                {insert}
                SELECT
                    '{dq_run_hour}' AS dq_run_hour
                    ,'{database_name}' AS database_name
                    ,'{schema_name}' AS schema_name
                    ,'{table_name}' AS table_name
                    ,'{table_filter}' AS table_filter
                    ,'empty_null' AS dq_name
                    ,x.dq_column AS dq_column
                    ,'{desc}' AS dq_description
                    ,CAST(x.empty_null_cnt AS VARCHAR) AS dq_tgt_value
                    ,'0' AS dq_src_value
                    ,'{threshold}' AS dq_threshold
                    ,CASE WHEN x.empty_null_cnt = 0 THEN true
                        ELSE false END AS is_pass
                    ,{stop} AS stop_on_failure
                    ,false AS is_dq_custom
                    ,{dq_key} AS dq_key
                    ,CURRENT_TIMESTAMP AS dq_start_tstamp
                    ,NULL AS dq_end_tstamp
                    ,'{db_username}' AS db_username
                    ,'{unix_username}' AS unix_username
                    ,'{env}' AS env
                    ,{trial} AS is_trial
                FROM
                (
                    {with_clause}
                ) x
        """.format(
                insert=vars['insert_sql'],
                dq_run_hour=vars['dq_run_hour'],
                database_name=vars['target_database_name'],
                schema_name=vars['target_schema_name'],
                table_name=vars['target_table_name'],
                table_filter=vars['target_filter'].replace("1=1","").replace("'", "''"),
                desc=description.replace("'", "''"),
                target_table=vars['target_table'],
                target_filter=vars['target_filter'],
                threshold=threshold,
                stop=stop_on_failure,
                dq_key=vars['dq_key'],
                db_username=vars['db_username'],
                unix_username=vars['unix_username'],
                env=vars['env'],
                with_clause=with_clause,
                trial=is_trial,
                )
        )

        return self.sqls

    def unique(self, dq_name, threshold, stop_on_failure, columns, is_trial, description, vars):
        # Default threshold to 0
        threshold = 0

        if len(columns) == 0:
            raise Exception('UNIQUE check requires [columns] to be specified')
        # This is for a single query for all columns
        select_list = []
        with_clause = """
                WITH subq AS
                (SELECT count(*) total_cnt
        """
        i = 1
        # Keep dq_columns as 1 element since we have a single query to do it for all columns
        # and expand later when checking for pass/fail result
        for column in columns:
            # For coalesce with default character, we need to replace single quote
            # for dq_column when inserting to dq_result_table
            with_clause += """
                    ,'{column1}'::VARCHAR AS col{i}_nm
                    ,count(distinct {column2}) AS col{i}_cnt
            """.format(column1=column.replace("'", "''"), column2=column, i=i)
            select_list.append("""
                SELECT total_cnt, col{i}_nm AS dq_column, col{i}_cnt AS distinct_cnt FROM subq
            """.format(i=i))
            i += 1
        with_clause += """
                FROM {target_table}
                WHERE {target_filter})
        """.format(
            target_table=vars['target_table'],
            target_filter=vars['target_filter'],
            )
        with_clause += ' UNION ALL '.join(select_list)
        self.sqls.append("""
                {insert}
                SELECT
                    '{dq_run_hour}' AS dq_run_hour
                    ,'{database_name}' AS database_name
                    ,'{schema_name}' AS schema_name
                    ,'{table_name}' AS table_name
                    ,'{table_filter}' AS table_filter
                    ,'unique' AS dq_name
                    ,x.dq_column AS dq_column
                    ,'{desc}' AS dq_description
                    ,CAST(x.distinct_cnt AS VARCHAR) AS dq_tgt_value
                    ,CAST(x.total_cnt AS VARCHAR) AS dq_src_value
                    ,'{threshold}' AS dq_threshold
                    ,CASE WHEN x.distinct_cnt = x.total_cnt THEN true
                        ELSE false END AS is_pass
                    ,{stop} AS stop_on_failure
                    ,false AS is_dq_custom
                    ,{dq_key} AS dq_key
                    ,CURRENT_TIMESTAMP AS dq_start_tstamp
                    ,NULL AS dq_end_tstamp
                    ,'{db_username}' AS db_username
                    ,'{unix_username}' AS unix_username
                    ,'{env}' AS env
                    ,{trial} AS is_trial
                FROM
                (
                    {with_clause}
                ) x
        """.format(
                insert=vars['insert_sql'],
                dq_run_hour=vars['dq_run_hour'],
                database_name=vars['target_database_name'],
                schema_name=vars['target_schema_name'],
                table_name=vars['target_table_name'],
                table_filter=vars['target_filter'].replace("1=1","").replace("'", "''"),
                desc=description.replace("'", "''"),
                target_table=vars['target_table'],
                target_filter=vars['target_filter'],
                threshold=threshold,
                stop=stop_on_failure,
                dq_key=vars['dq_key'],
                db_username=vars['db_username'],
                unix_username=vars['unix_username'],
                env=vars['env'],
                trial=is_trial,
                with_clause=with_clause,
                )
        )

        return self.sqls

    def up_to_date(self, dq_name, threshold, stop_on_failure, columns, is_trial, description, vars):
        if len(columns) == 0:
            raise Exception('UP_TO_DATE check requires [columns] to be specified')
        for column in columns:
            self.sqls.append("""
                {insert}
                SELECT
                    '{dq_run_hour}' AS dq_run_hour
                    ,'{database_name}' AS database_name
                    ,'{schema_name}' AS schema_name
                    ,'{table_name}' AS table_name
                    ,'{table_filter}' AS table_filter
                    ,'up_to_date' AS dq_name
                    ,'{column_desc}' AS dq_column
                    ,'{desc}' AS dq_description
                    ,CAST(MAX({column}::date) AS VARCHAR) AS dq_tgt_value
                    ,CAST(CURRENT_TIMESTAMP::date AS VARCHAR) AS dq_src_value
                    ,NULL AS dq_threshold
                    ,CASE WHEN MAX({column}::date) >= CURRENT_TIMESTAMP::date THEN true
                        ELSE false END AS is_pass
                    ,{stop} AS stop_on_failure
                    ,false AS is_dq_custom
                    ,{dq_key} AS dq_key
                    ,CURRENT_TIMESTAMP AS dq_start_tstamp
                    ,NULL AS dq_end_tstamp
                    ,'{db_username}' AS db_username
                    ,'{unix_username}' AS unix_username
                    ,'{env}' AS env
                    ,{trial} AS is_trial
                FROM
                    {target_table}
                WHERE
                    {target_filter}
                ;
            """.format(
                insert=vars['insert_sql'],
                dq_run_hour=vars['dq_run_hour'],
                database_name=vars['target_database_name'],
                schema_name=vars['target_schema_name'],
                table_name=vars['target_table_name'],
                table_filter=vars['target_filter'].replace("1=1","").replace("'", "''"),
                target_table=vars['target_table'],
                target_filter=vars['target_filter'],
                column_desc=column.replace("'", "''"),
                column=column,
                desc=description.replace("'", "''"),
                threshold=threshold,
                stop=stop_on_failure,
                dq_key=vars['dq_key'],
                db_username=vars['db_username'],
                unix_username=vars['unix_username'],
                env=vars['env'],
                trial=is_trial,
                )
            )

        return self.sqls

    def day_to_day(self, dq_name, threshold, stop_on_failure, columns, is_trial, description, vars,
        group_by=None, num_days=None, threshold_min=None):
        if len(columns) == 0:
            raise Exception('DAY_TO_DAY check requires [columns] to be specified')
        if group_by is None:
            raise Exception('DAY_TO_DAY check requires [group_by] to be specified')
        if num_days is None:
            raise Exception('DAY_TO_DAY check requires [num_days] to be specified')

        # If there's a min threshold specified, execute a different logic
        # and no more for up and down
        if threshold_min is not None:
            compare_logic = 'abs(dq_tgt_value - dq_src_value)/(dq_src_value*1.0) between {tm} and {t}'.format(
                t=threshold,
                tm=threshold_min,
                )
            threshold = '{tm},{t}'.format(
                t=threshold,
                tm=threshold_min,
                )
            trending_type = 'range'
        else:
            # Feature for checking up and down trending
            if '+' in str(threshold):
                compare_logic = '(dq_tgt_value - dq_src_value)/(dq_src_value*1.0) between 0 and {t}'.format(
                    t=threshold.replace('+', '')
                    )
                trending_type = 'upward'
            elif '-' in str(threshold):
                compare_logic = '(dq_tgt_value - dq_src_value)/(dq_src_value*1.0) between {t} and 0'.format(
                    t=threshold
                    )                
                trending_type = 'downward'
            else:
                compare_logic = 'abs(dq_tgt_value - dq_src_value)/(dq_src_value*1.0) <= {t}'.format(
                    t=threshold
                    )
                trending_type = 'absolute'

        # Keep group_by AS single column in DQ
        group_by1 = group_by.replace(",", "|| ',' || ")

        for column in columns:
            self.sqls.append("""
                -- Trending type: {trending_type}
                {insert}
                WITH subq AS
                (SELECT {group_by} AS col, {column} AS dq_tgt_value
                FROM {target_table}
                WHERE {target_filter}
                GROUP BY {group_by}
                ORDER BY {group_by} DESC
                LIMIT {num_days}
                ),
                subq_prev AS
                (
                SELECT
                    col,
                    dq_tgt_value,
                    LEAD(dq_tgt_value) OVER(ORDER BY col DESC) dq_src_value
                FROM subq 
                )
                SELECT
                    '{dq_run_hour}' AS dq_run_hour
                    ,'{database_name}' AS database_name
                    ,'{schema_name}' AS schema_name
                    ,'{table_name}' AS table_name
                    ,'{table_filter}' AS table_filter
                    ,'day_to_day' AS dq_name
                    ,'{column_desc} [' || col || ']' AS dq_column
                    ,'{desc}' AS dq_description
                    ,dq_tgt_value AS dq_tgt_value
                    ,dq_src_value AS dq_src_value
                    ,'{threshold}' AS dq_threshold
                    ,is_pass
                    ,{stop} AS stop_on_failure
                    ,false AS is_dq_custom
                    ,{dq_key} AS dq_key
                    ,CURRENT_TIMESTAMP AS dq_start_tstamp
                    ,NULL AS dq_end_tstamp
                    ,'{db_username}' AS db_username
                    ,'{unix_username}' AS unix_username
                    ,'{env}' AS env
                    ,{trial} AS is_trial
                FROM
                    (SELECT
                        col
                        ,dq_tgt_value
                        ,dq_src_value
                        ,CASE WHEN dq_src_value is NULL THEN true
                            WHEN dq_src_value = 0 THEN false
                            WHEN dq_tgt_value = 0 THEN false
                            WHEN {logic} THEN true
                            ELSE false END AS is_pass
                    FROM
                        subq_prev
                    ) x
                ;
            """.format(
                trending_type=trending_type,
                insert=vars['insert_sql'],
                dq_run_hour=vars['dq_run_hour'],
                database_name=vars['target_database_name'],
                schema_name=vars['target_schema_name'],
                table_name=vars['target_table_name'],
                table_filter=vars['target_filter'].replace("1=1","").replace("'", "''"),
                target_table=vars['target_table'],
                target_filter=vars['target_filter'],
                column_desc=column.replace("'", "''"),
                column=column,
                desc=description.replace("'", "''") + " [groupby (" + group_by + ") for " + str(num_days) + " days]",
                threshold=threshold,
                group_by=group_by1,
                num_days=num_days,
                logic=compare_logic,
                stop=stop_on_failure,
                dq_key=vars['dq_key'],
                db_username=vars['db_username'],
                unix_username=vars['unix_username'],
                env=vars['env'],
                trial=is_trial,
                )
            )

        return self.sqls

    def missing_dates(self, dq_name, threshold, stop_on_failure, columns, is_trial, description, vars):
        if len(columns) == 0:
            raise Exception('MISSING_DATES check requires [columns] to be specified')
        for column in columns:
            self.sqls.append("""
                {insert}
                WITH max_date AS
                (
                SELECT MAX({column}::date) AS max_date
                FROM {target_table}
                WHERE {target_filter}
                )
                ,all_dates AS
                (
                SELECT
                  DATEADD(day, '-' || 
                    ROW_NUMBER() OVER (ORDER BY NULL), DATEADD(day, '+1', m.max_date)) AS date_col
                FROM table (generator(rowcount => 90)),
                    max_date m
                )
                SELECT
                    '{dq_run_hour}' AS dq_run_hour
                    ,'{database_name}' AS database_name
                    ,'{schema_name}' AS schema_name
                    ,'{table_name}' AS table_name
                    ,'{table_filter}' AS table_filter
                    ,'missing_dates' AS dq_name
                    ,'{column_desc}' AS dq_column
                    ,'{desc}' AS dq_description
                    ,total_cnt AS dq_tgt_value
                    ,0 AS dq_src_value
                    ,NULL AS dq_threshold
                    ,CASE WHEN total_cnt = 0 THEN true
                        ELSE false END AS is_pass
                    ,{stop} AS stop_on_failure
                    ,false AS is_dq_custom
                    ,{dq_key} AS dq_key
                    ,CURRENT_TIMESTAMP AS dq_start_tstamp
                    ,NULL AS dq_end_tstamp
                    ,'{db_username}' AS db_username
                    ,'{unix_username}' AS unix_username
                    ,'{env}' AS env
                    ,{trial} AS is_trial
                FROM
                    (
                    SELECT 
                        SUM(CASE WHEN b.date_col IS NULL THEN 1 ELSE 0 END) AS total_cnt
                    FROM
                        all_dates a
                        LEFT JOIN
                        (
                        SELECT DISTINCT {column}::date AS date_col
                        FROM {target_table}
                        WHERE {target_filter}
                        ) b
                        ON (a.date_col = b.date_col)
                    ) x
                ;
            """.format(
                insert=vars['insert_sql'],
                dq_run_hour=vars['dq_run_hour'],
                database_name=vars['target_database_name'],
                schema_name=vars['target_schema_name'],
                table_name=vars['target_table_name'],
                table_filter=vars['target_filter'].replace("1=1","").replace("'", "''"),
                target_table=vars['target_table'],
                target_filter=vars['target_filter'],
                column_desc=column.replace("'", "''"),
                column=column,
                desc=description.replace("'", "''"),
                threshold=threshold,
                stop=stop_on_failure,
                dq_key=vars['dq_key'],
                db_username=vars['db_username'],
                unix_username=vars['unix_username'],
                env=vars['env'],
                trial=is_trial,
                )
            )

        return self.sqls

    def get_stddev_setup(self, column, vars):
        return '''
            INSERT INTO {dq_table}
            SELECT
                dq_run_hour
                ,database_name
                ,schema_name
                ,table_name
                ,table_filter
                ,'std_dev' AS dq_name
                ,dq_column
                ,'Initial trending for std_dev' AS dq_description
                ,dq_tgt_value
                ,NULL AS dq_src_value
                ,NULL AS dq_threshold
                ,false AS is_pass
                ,false AS stop_on_failure
                ,false AS is_dq_custom
                ,NULL AS dq_key
                ,NULL AS dq_start_tstamp
                ,NULL AS dq_end_tstamp
                ,NULL AS db_username
                ,NULL AS unix_username
                ,env
                ,true AS is_trial
            FROM
                {dq_table_prod}
            WHERE
                database_name = '{database}'
                AND schema_name = '{schema}'
                AND table_name = '{table}'
                AND dq_name = 'trending'
                AND dq_column = '{dq_column}'
                AND env = '{env}'
                AND dq_run_hour > current_date - interval '9 week'
            ;
        '''.format(
            dq_table=vars['dq_table'],
            dq_table_prod=vars['dq_table_prod'],
            database=vars['target_database_name'],
            schema=vars['target_schema_name'],
            table=vars['target_table_name'],
            dq_column=column,
            env=vars['env'],
            )

    def std_dev(self, dq_name, threshold, stop_on_failure, columns, is_trial, description, vars):
        if len(columns) == 0:
            raise Exception('STD_DEV check requires [columns] to be specified')

        # Top and bottom standard deviation range
        calc_logic_top = 'MAX(last_src_value) + AVG(diff)::INT + ({t} * STDDEV(diff))::INT'.format(
            t=threshold.replace('+', '')
            )
        calc_logic_bottom = 'MAX(last_src_value) - AVG(diff)::INT - ({t} * STDDEV(diff))::INT'.format(
            t=threshold.replace('+', '')
            )
        
        # Feature for checking up and down trending
        #commented DM-6823 std_dev_logic = '((t.dq_tgt_value::INT - s.last_src_value - s.avg_diff)/s.std_dev_diff)::DECIMAL(10,2)'
        if '+' in str(threshold):
            compare_logic = 't.dq_tgt_value::INT < s.top_value::INT'
            src_logic = calc_logic_top
            trending_type = 'upward'
        elif '-' in str(threshold):
            compare_logic = 't.dq_tgt_value::INT > s.bottom_value'
            src_logic = calc_logic_bottom
            trending_type = 'downward'
        else:
            compare_logic = 't.dq_tgt_value between s.bottom_value::INT and s.top_value::INT'
            src_logic = 'CAST({bottom} AS VARCHAR) || \'|\' || CAST({top} AS VARCHAR)'.format(
                top=calc_logic_top,
                bottom=calc_logic_bottom,
                )
            trending_type = 'absolute'

        for column in columns:
            self.sqls.append("""
                -- Trending type: {trending_type}
                {insert}
                WITH dq_dedup AS
                (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY dq_run_hour::DATE ORDER BY dq_run_hour DESC) rnk
                FROM {dq_table_prod}
                WHERE database_name = '{database_name}' AND schema_name = '{schema_name}' AND table_name = '{table_name}'
                    AND dq_name = 'std_dev' AND dq_column = '{column}'
                    AND env = '{env}'
                    AND (dq_run_hour::date = current_date - interval '7 day'
                      OR dq_run_hour::date = current_date - interval '14 day'
                      OR dq_run_hour::date = current_date - interval '21 day'
                      OR dq_run_hour::date = current_date - interval '28 day'
                      OR dq_run_hour::date = current_date - interval '35 day'
                      OR dq_run_hour::date = current_date - interval '42 day'
                      OR dq_run_hour::date = current_date - interval '49 day'
                      OR dq_run_hour::date = current_date - interval '56 day')
                )
                SELECT
                    '{dq_run_hour}' AS dq_run_hour
                    ,'{database_name}' AS database_name
                    ,'{schema_name}' AS schema_name
                    ,'{table_name}' AS table_name
                    ,'{table_filter}' AS table_filter
                    ,'std_dev' AS dq_name
                    ,'{column_desc}' AS dq_column
                    ,'{desc}' AS dq_description
                    ,CAST(t.dq_tgt_value AS VARCHAR) AS dq_tgt_value
                    ,CASE WHEN s.last_src_value IS NULL THEN CAST(s.dq_src_value AS VARCHAR)
                        ELSE CAST(s.dq_src_value AS VARCHAR) || 
                            ' (last_src=' || CAST(s.last_src_value AS VARCHAR) || ')' ||
                            ' (avg_diff=' || CAST(s.avg_diff AS VARCHAR) || ')' ||
                            ' (src_stddev=' || CAST(s.std_dev_diff AS VARCHAR) || ')' ||
                            ' (cnt=' || CAST(s.num_of_weeks AS VARCHAR) || ')'
                        END AS dq_src_value
                    ,'{threshold}' AS dq_threshold
                    ,CASE WHEN s.dq_src_value IS NULL THEN true
                        WHEN {compare_logic} THEN true
                        ELSE false END AS is_pass
                    ,{stop} AS stop_on_failure
                    ,false AS is_dq_custom
                    ,{dq_key} AS dq_key
                    ,CURRENT_TIMESTAMP AS dq_start_tstamp
                    ,NULL AS dq_end_tstamp
                    ,'{db_username}' AS db_username
                    ,'{unix_username}' AS unix_username
                    ,'{env}' AS env
                    ,{trial} AS is_trial
                FROM
                    (
                        SELECT {column} AS dq_tgt_value
                        FROM {target_table}
                        WHERE {target_filter}
                    ) t LEFT OUTER JOIN
                    (
                        SELECT
                            table_name
                            ,{src_logic} AS dq_src_value
                            ,MAX(last_src_value) AS last_src_value
                            ,{calc_top} AS top_value
                            ,{calc_bottom} AS bottom_value
                            ,AVG(diff)::INT AS avg_diff
                            ,STDDEV(diff)::INT AS std_dev_diff
                            ,COUNT(*) AS num_of_weeks
                        FROM
                        (
                            SELECT
                                table_name
                                ,dq_run_hour
                                ,CASE WHEN (dq_run_hour::DATE = current_date - interval '7 day')
                                    THEN dq_tgt_value::INT ELSE 0 END last_src_value
                                ,dq_tgt_value::INT AS dq_run_value
                                ,LAG(dq_tgt_value::INT) OVER (ORDER BY dq_run_hour) AS prev_dq_run_value
                                ,dq_tgt_value::INT - LAG(dq_tgt_value::INT) OVER (ORDER BY dq_run_hour ASC) AS diff
                            FROM dq_dedup
                            WHERE rnk = 1
                            ORDER BY dq_run_hour ASC 
                        ) sc
                        GROUP BY 1
                    ) s
                    ON (1=1)
                ;
            """.format(
                trending_type=trending_type,
                insert=vars['insert_sql'],
                dq_run_hour=vars['dq_run_hour'],
                database_name=vars['target_database_name'],
                schema_name=vars['target_schema_name'],
                table_name=vars['target_table_name'],
                table_filter=vars['target_filter'].replace("1=1","").replace("'", "''"),
                column_desc=column.replace("'", "''"),
                column=column,
                desc=description.replace("'", "''"),
                target_table=vars['target_table'],
                target_filter=vars['target_filter'],
                threshold=threshold,
                compare_logic=compare_logic,
                src_logic=src_logic,
                calc_top=calc_logic_top,
                calc_bottom=calc_logic_bottom,
                stop=stop_on_failure,
                dq_key=vars['dq_key'],
                dq_table=vars['dq_table'],
                dq_table_prod=vars['dq_table_prod'],
                db_username=vars['db_username'],
                unix_username=vars['unix_username'],
                env=vars['env'],
                trial=is_trial,
                )
            )

        return self.sqls
