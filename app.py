# stdlib
import json
from dataclasses import dataclass
from itertools import chain
from typing import Dict, List
from urllib.parse import parse_qs, urlparse

# third party
import plotly.express as px
import requests
import streamlit as st
import streamlit.components.v1 as components
from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import connect

st.set_page_config(
    page_title=getattr(st.session_state, 'selected_page', 'dbt Semantic Layer'),
    layout='wide',
)


########### QUERIES ###########

queries = {
    'metrics': '''
select *
from {{
    semantic_layer.metrics()
}}
''',
    'dimensions': '''
select *
from {{{{
    semantic_layer.dimensions(
        metrics={metrics}
    )
}}}}
''',
    'dimension_values': '''
select *
from {{{{
    semantic_layer.dimension_values(
        metrics={metrics},
        group_by='{dimension}'
    )
}}}}
''',
    'queryable_granularities': '''
select *
from {{{{
    semantic_layer.queryable_granularities(
        metrics={metrics}
    )
}}}}
''',
    'metrics_for_dimensions': '''
select *
from {{{{
    semantic_layer.metrics_for_dimensions(
        group_by={dimensions}
    )
}}}}
'''
}

########### HELPERS ###########

def keys_exist_in_dict(keys_list, dct):
    return all(key in dct for key in keys_list)


def get_shared_elements(all_elements: List[List]):
    if len(all_elements) == 0:
        return []
    
    try:
        unique = set(all_elements[0]).intersection(*all_elements[1:])
    except IndexError:
        unique = set(all_elements[0])
        
    return list(unique)

########### SEMANTIC LAYER QUERY ###########

class SemanticLayerQuery:
    def __init__(self, state: Dict):
        self.state = state
        self._classify_dimensions()
        self._format_metrics()
        self._format_dimensions()
        self._format_filters()
        self._format_order_by()
        
    def _has_type_dimension(self, dim_type: str):
        return dim_type in self.dimensions.keys()
        
    @property
    def has_time_dimension(self):
        return self._has_type_dimension('time')
    
    @property
    def has_entity_dimension(self):
        return self._has_type_dimension('entity')
    
    @property
    def has_categorical_dimension(self):
        return self._has_type_dimension('categorical')
    
    @property
    def all_dimensions(self):
        return list(chain.from_iterable(self.dimensions.values()))
    
    @property
    def dimensions_x_time(self):
        return self.dimensions['entity'] + self.dimensions['categorical']
    
    @property
    def all_columns(self):
        return self.all_dimensions + self.metrics
    
    def _is_dim_type(self, dimension_type, dimension):
        try:
            return dimension_type.lower() == self.state.dimension_dict[dimension]['type'].lower()
        except KeyError:
            return False
        
    def _classify_dimensions(self):
        self.dimensions = {}
        for dimension in self.state.selected_dimensions:
            try:
                dim_type = self.state.dimension_dict[dimension]['type'].lower()
            except KeyError:
                pass
            else:
                if dim_type not in self.dimensions:
                    self.dimensions[dim_type] = []
                if dim_type == 'time':
                    dimension = f'{dimension}__{self.state.selected_grain}'
                self.dimensions[dim_type].append(dimension)
        
    def _format_metrics(self) -> None:
        self.metrics = self.state.selected_metrics
    
    def _format_dimensions(self) -> None:
        formatted_dimensions = []
        for dim in self.state.selected_dimensions:
            if self._is_dim_type('time', dim):
                formatted_dimensions.append(
                    f'{dim}__{self.state.selected_grain}'
                )
            else:
                formatted_dimensions.append(dim)
        self._group_by = formatted_dimensions

    def _create_list_of_lists(self, sql_type: str, components: List[str]):
        results = []
        for i in range(10):
            keys = [f'{sql_type}_{component}_{i}' for component in components]
            if keys_exist_in_dict(keys, self.state):
                results.append([self.state[key] for key in keys])
            else:
                break
        return results
        
    def _format_filters(self) -> None:
        filters = self._create_list_of_lists('where', ['column', 'operator', 'condition'])
        formatted_filters = []
        for column, operator, condition in filters:
            if self._is_dim_type('time', column):
                dim_class = f"TimeDimension('{column}', '{self.state.get('selected_grain', 'day').upper()}')"
            elif self._is_dim_type('entity', column):
                dim_class = f"Entity('{column}')"
            else:
                dim_class = f"Dimension('{column}')"
            formatted_filters.append(
                f"{{{{ {dim_class} }}}} {operator} {condition}"
            )
        self._where = ' AND '.join(formatted_filters)
        
    def _format_order_by(self) -> None:
        orders = self._create_list_of_lists('order', ['column', 'direction'])
        formatted_orders = []
        for column, direction in orders:
            if self._is_dim_type('time', column):
                column = f'{column}__{self.state.selected_grain}'
            if direction.lower() == 'desc':
                formatted_orders.append(f'-{column}')
            else:
                formatted_orders.append(column)
        self._order_by = formatted_orders
        
    @property
    def _query_inner(self):
        text = f'metrics={self.metrics}'
        if len(self._group_by) > 0:
            text += f',\n        group_by={self._group_by}'
        if len(self._where) > 0:
            text += f',\n        where="{self._where}"'
        if len(self._order_by) > 0:
            text += f',\n        order_by={self._order_by}'
        if self.state.selected_limit is not None and self.state.selected_limit != 0:
            text += f',\n        limit={self.state.selected_limit}'
        if self.state.selected_explain:
            text += f',\n        explain={self.state.selected_explain}'
        return text
        
    @property
    def query(self):
        sql = f'''
select *
from {{{{
    semantic_layer.query(
        {self._query_inner}
    )
}}}}
        '''
        return sql

########### CLIENT ###########

@dataclass
class ConnAttr:
    host: str  # "grpc+tls:semantic-layer.cloud.getdbt.com:443"
    params: dict  # {"environmentId": 42}
    auth_header: str  # "Bearer dbts_thisismyprivateservicetoken"


@st.cache_data
def get_connection_attributes(uri):
    """Helper function to convert the JDBC url into ConnAttr."""
    parsed = urlparse(uri)
    params = {k.lower(): v[0] for k, v in parse_qs(parsed.query).items()}
    try:
        token = params.pop('token')
    except KeyError:
        st.error('Token is missing from the JDBC URL.')
    else:
        return ConnAttr(
            host=parsed.path.replace("arrow-flight-sql", "grpc")
            if params.pop("useencryption", None) == "false"
            else parsed.path.replace("arrow-flight-sql", "grpc+tls"),
            params=params,
            auth_header=f"Bearer {token}",
        )


@st.cache_data(show_spinner=False)
def submit_query(_conn_attr: ConnAttr, query: str, stop_on_error: bool = False):
    with connect(
        _conn_attr.host,
        db_kwargs={
            DatabaseOptions.AUTHORIZATION_HEADER.value: _conn_attr.auth_header,
            **{
                f"{DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}{k}": v
                for k, v in _conn_attr.params.items()
            },
        },
    ) as conn, conn.cursor() as cur:
        try:
            cur.execute(query)
        except Exception as e:
            st.error(f'There was an error executing the query:\n\n"{query}"\n\nError message: {e}')
            if stop_on_error:
                st.stop()
        else:
            return cur.fetch_df()  # fetches as Pandas DF, can also do fetch_arrow_table

########### CHART ###########

CHART_TYPE_FIELDS = {
    'line': ['x', 'y', 'y2', 'color', 'facet_row', 'facet_col'],
    'bar': ['x', 'y', 'color', 'orientation', 'barmode'],
    'pie': ['values', 'names'],
    'area': ['x', 'y', 'color'],
    'scatter': ['x', 'y', 'color', 'size', 'facet_col', 'facet_row', 'trendline'],
    'histogram': ['x', 'nbins', 'histfunc'],
}

        
def create_chart(df, slq: SemanticLayerQuery):
    
    
    def _can_add_field(selections, available):
        return len(selections) < len(available)
    
    
    def _available_options(selections, available):
        return [option for option in available if option not in selections]

    
    def _sort_dataframe(df, slq: SemanticLayerQuery):
        try:
            time_dimensions = [col for col in df.columns if col in slq.dimensions['time']]
        except KeyError:
            return df
        else:
            if len(time_dimensions) > 0:
                col = time_dimensions[0]
                is_sorted = df[col].is_monotonic_increasing
                if not is_sorted:
                    df = df.sort_values(by=col)
            return df

    col1, col2 = st.columns([.2, .8])
    
    # Create default chart types
    if slq.has_time_dimension:
        chart_types = ['line', 'area', 'bar']
    elif slq.has_entity_dimension:
        if len(slq.metrics) >= 2:
            chart_types = ['scatter', 'bar']
        else:
            chart_types = ['bar', 'pie', 'histogram']
    else:
        chart_types = ['bar', 'pie', 'histogram', 'scatter']

    selected_chart_type = col1.selectbox(
        label='Select Chart Type',
        options=chart_types,
        key='selected_chart_type',
    )

    chart_config = {}
    
    for field in CHART_TYPE_FIELDS[selected_chart_type]:
        
        selected_dimensions = [
            col for col in chart_config.values() if col in slq.all_dimensions
        ]
        selected_metrics = [
            col for col in chart_config.values() if col in slq.metrics
        ]
        
        
        if field == 'x':
            if selected_chart_type in ['scatter', 'histogram']:
                options = slq.metrics
            elif slq.has_time_dimension:
                options = slq.dimensions['time']
            else:
                options = slq.all_dimensions
            x = col1.selectbox(
                label='X-Axis',
                options=options,
                placeholder='Select Dimension',
                key='chart_config_x',
            )
            chart_config['x'] = x
            
        if field == 'y':
            if len(slq.metrics) == 1 or selected_chart_type != 'line':
                widget = 'selectbox'
                y_kwargs = {}
            else:
                widget = 'multiselect'
                y_kwargs = {'default': slq.metrics[0]}
            y = getattr(col1, widget)(
                label='Y-Axis',
                options=[m for m in slq.metrics if m not in chart_config.values()],
                placeholder='Select Metric',
                key='chart_config_y',
                **y_kwargs,
            )
            chart_config['y'] = y
            
        # if len(slq.metrics) > 1:
        #     # if col1.button('Add Secondary Y-Axis', key='y_axis_button'):
        #     y2 = col1.selectbox(
        #         label='Y-Axis 2',
        #         options=[m for m in slq.metrics if m not in chart_config.values()],
        #         placeholder='Select Secondary Axis',
        #         key='chart_config_y2',
        #     )
        #     chart_config['y2'] = y2
                
        if field == 'values':
            values = col1.selectbox(
                label='Values',
                options=slq.metrics,
                placeholder='Select Value',
                key='chart_config_values',
            )
            chart_config['values'] = values
            
        if field == 'names':
            names = col1.selectbox(
                label='Select Dimension',
                options=slq.all_dimensions,
                key='chart_config_names',
            )
            chart_config['names'] = names
            
        if _can_add_field(chart_config.values(), slq.all_columns) and field == 'color':
            color = col1.selectbox(
                label='Color',
                options=_available_options(chart_config.values(), slq.all_columns),
                placeholder='Select Color',
                key='chart_config_color',
            )
            chart_config['color'] = color
            
        if _can_add_field(selected_metrics, slq.metrics) and field == 'size':
            size = col1.selectbox(
                label='Size',
                options=_available_options(selected_metrics, slq.metrics),
                placeholder='Select Size',
                key='chart_config_size',
            )
            chart_config['size'] = size
            
        if _can_add_field(selected_dimensions, slq.all_dimensions) and field == 'facet_col':
            facet_col = col1.selectbox(
                label='Facet Column',
                options=[None] + _available_options(selected_dimensions, slq.all_dimensions),
                placeholder='Select Facet Column',
                key='chart_config_facet_col',
            )
            chart_config['facet_col'] = facet_col
            
        if _can_add_field(selected_dimensions, slq.all_dimensions) and field == 'facet_row':
            facet_row = col1.selectbox(
                label='Facet Row',
                options=[None] + _available_options(selected_dimensions, slq.all_dimensions),
                placeholder='Select Facet Row',
                key='chart_config_facet_row',
            )
            chart_config['facet_row'] = facet_row
            
        if field == 'histfunc':
            histfunc = col1.selectbox(
                label='Histogram Function',
                options=['sum', 'count', 'avg'],
                placeholder='Select Function',
                key='chart_config_histfunc',
            )
            chart_config['histfunc'] = histfunc
            
        if field == 'nbins':
            nbins = col1.number_input(
                label='Number of Bins',
                min_value=0,
                key='chart_config_nbins',
                value=0,
                help='If set to 0, the number of bins will be determined automatically',
            )
            chart_config['nbins'] = nbins
            
        # if field == 'trendline':
        #     trendline = col1.selectbox(
        #         label='Select Trendline',
        #         options=[None, 'ols'],
        #         key='chart_config_trendline',
        #     )
        #     chart_config['trendline'] = trendline
            
        if field == 'orientation':
            orientation = col1.selectbox(
                label='Select Orientation',
                options=['Vertical', 'Horizontal'],
                key='chart_config_orientation',
            )
            chart_config['orientation'] = orientation[:1].lower()
            if chart_config['orientation'] == 'h':
                x = chart_config.pop('x')
                y = chart_config.pop('y')
                chart_config['x'] = y
                chart_config['y'] = x
        
        if field == 'barmode' and len(slq.all_dimensions) > 1:
            barmode = col1.selectbox(
                label='Select Bar Mode',
                options=['group', 'stack'],
                key='chart_config_barmode',
            )
            chart_config['barmode'] = barmode
        
    with col2:
        # TODO: Sort time dimension automatically if not explicitly done in query
        
        df = _sort_dataframe(st.session_state.df, st.session_state.slq)
        fig = getattr(px, selected_chart_type)(df, **chart_config)
        st.plotly_chart(fig, theme='streamlit', use_container_width=True)
    
########### HOMEPAGE VIEW ###########

def homepage_view():
    

    def prepare_app():
        
        with st.spinner(f'Gathering Metrics...'):
            df = submit_query(st.session_state.conn, queries['metrics'])
            if df is not None:
                df.columns = [col.lower() for col in df.columns]
                try:
                    df.set_index(keys='name', inplace=True)
                except KeyError:
                    
                    # Query worked, but nothing returned
                    st.warning(
                        'No Metrics returned!  Ensure your project has metrics defined '
                        'and a production job has been run successfully.'
                    )
                else:
                    df['dimensions'] = df['dimensions'].str.split(', ')
                    df['queryable_granularities'] = (
                        df['queryable_granularities'].str.split(', ')
                    )
                    df['type_params'] = df['type_params'].apply(
                        lambda x: json.loads(x) if x else None
                    )
                    st.session_state.metric_dict = df.to_dict(orient='index')
                    st.success('Success!  Explore the rest of the app!')

    st.markdown('# Explore the dbt Semantic Layer')

    st.markdown(
        """
        Use this app to query and view the metrics defined in your dbt project. It's important to note that this app assumes that you're using the new
        Semantic Layer, powered by [MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow).  The previous semantic layer used the `dbt_metrics`
        package, which has been deprecated and is no longer supported for `dbt-core>=1.6`.
        
        ---
        
        To get started, input your `JDBC_URL` below.  You can find this in your project settings when setting up the Semantic Layer.
        After hitting Enter, wait until a success message appears indicating that the application has successfully retrieved your project's metrics information.
        """
    )


    st.text_input(
        label='JDBC URL',
        value='',
        key='jdbc_url',
        help='JDBC URL is found when configuring the semantic layer at the project level',
    )

    if st.session_state.jdbc_url != '':
        st.cache_data.clear()
        st.session_state.conn = get_connection_attributes(st.session_state.jdbc_url)
        if 'conn' in st.session_state and st.session_state.conn is not None:
            prepare_app()

    st.markdown(
        """
        ---
        **👈 Now, select a page from the sidebar** to explore the Semantic Layer!

        ### Want to learn more?
        - Get started with the [dbt Semantic Layer](https://docs.getdbt.com/docs/use-dbt-semantic-layer/quickstart-sl)
        - Understand how to [build your metrics](https://docs.getdbt.com/docs/build/build-metrics-intro)
        - View the [Semantic Layer API](https://docs.getdbt.com/docs/dbt-cloud-apis/sl-api-overview)
        - Brief Demo 👇
    """
    )

    components.html(
        '''<div style="position: relative; padding-bottom: 77.25321888412017%; height: 0;"><iframe src="https://www.loom.com/embed/90419fc9aa1e4680a43525a386645a96?sid=4c3f76ff-21e5-4a86-82e8-c03489b646d5" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>''',
        height=600
    )

########### EXPLORE JDBC API VIEW ###########

def explore_jdbc_api_view():

    if 'conn' not in st.session_state or st.session_state.conn is None:
        st.warning('Go to home page and enter your JDBC URL')
        st.stop()
        
    if 'metric_dict' not in st.session_state:
        st.warning(
            'No metrics found.  Ensure your project has metrics defined and a production '
            'job has been run successfully.'
        )
        st.stop()

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        'Metrics',
        'Dimensions',
        'Dimension Values',
        'Time Granularities',
        'Common Metric Dimensions',
    ])

    with tab1:
        st.info('Use this query to fetch all defined metrics in your dbt project.')
        query = queries['metrics']
        st.code(query)
        if st.button('Submit Query', key='explore_submit_1'):
            df = submit_query(st.session_state.conn, query, True)
            st.dataframe(df, use_container_width=True)
            
    with tab2:
        st.info('Use this query to fetch all dimensions for a metric.')
        metrics = st.multiselect(
            label='Select Metric(s)',
            options=sorted(st.session_state.metric_dict.keys()),
            default=None,
            placeholder='Select a Metric',
            key='explore_metric_2'
        )
        query = queries['dimensions'].format(
            **{'metrics': metrics}
        )
        st.code(query)
        if st.button('Submit Query', key='explore_submit_2'):
            df = submit_query(st.session_state.conn, query, True)
            # Need to see if we can add metrics to the dataframe
            st.dataframe(df, use_container_width=True)
        
    with tab3:
        st.info('Use this query to fetch dimension values for one or multiple metrics and single dimension.')
        metrics = st.multiselect(
            label='Select Metric(s)',
            options=sorted(st.session_state.metric_dict.keys()),
            default=None,
            placeholder='Select a Metric',
            key='explore_metric_3',
        )
        all_dimensions = [
            v['dimensions'] for k, v in st.session_state.metric_dict.items()
            if k in metrics
        ]
        unique_dimensions = get_shared_elements(all_dimensions)
        dimension = st.selectbox(
            label='Select Dimension',
            options=sorted(unique_dimensions),
            placeholder='Select a dimension'
        )
        query = queries['dimension_values'].format(
            **{'metrics': metrics, 'dimension': dimension}
        )
        st.code(query)
        if st.button('Submit Query', key='explore_submit_3'):
            df = submit_query(st.session_state.conn, query, True)
            # Need to see if we can add metrics to the dataframe
            st.dataframe(df, use_container_width=True)

    with tab4:
        st.info('Use this query to fetch queryable granularities for a list of metrics. This argument allows you to only show the time granularities that make sense for the source model that the metrics are built off of.')
        metrics = st.multiselect(
            label='Select Metric(s)',
            options=sorted(st.session_state.metric_dict.keys()),
            default=None,
            placeholder='Select a Metric',
            key='explore_metric_4'
        )
        query = queries['queryable_granularities'].format(
            **{'metrics': metrics}
        )
        st.code(query)
        if st.button('Submit Query', key='explore_submit_4'):
            df = submit_query(st.session_state.conn, query, True)
            # Need to see if we can add metrics to the dataframe
            st.dataframe(df, use_container_width=True)
            
    with tab5:
        st.info('Use this query to fetch available metrics given dimensions. This command is essentially the opposite of getting dimensions given a list of metrics.')
        all_dimensions = [v['dimensions'] for k, v in st.session_state.metric_dict.items()]
        unique_dimensions = get_shared_elements(all_dimensions)
        dimensions = st.multiselect(
            label='Select Dimension(s)',
            options=sorted(unique_dimensions),
            default=None,
            placeholder='Select a dimension'
        )
        query = queries['metrics_for_dimensions'].format(
            **{'dimensions': dimensions}
        )
        st.code(query)
        if st.button('Submit Query', key='explore_submit_5'):
            df = submit_query(st.session_state.conn, query, True)
            # Need to see if we can add metrics to the dataframe
            st.dataframe(df, use_container_width=True)

########### QUERY METRICS VIEW ###########

def query_metrics_view():

    if 'conn' not in st.session_state or st.session_state.conn is None:
        st.warning('Go to home page and enter your JDBC URL')
        st.stop()
        
    if 'metric_dict' not in st.session_state:
        st.warning(
            'No metrics found.  Ensure your project has metrics defined and a production '
            'job has been run successfully.'
        )
        st.stop()

    def get_time_length(interval):
        time_lengths = {
            'day': 1,
            'week': 7,
            'month': 30,
            'quarter': 90,
            'year': 365
        }
        return time_lengths.get(interval, 0)


    def sort_by_time_length(time_intervals):
        return sorted(time_intervals, key=lambda x: get_time_length(x))


    def add_where_state():
        st.session_state.where_items += 1
        

    def subtract_where_state():
        st.session_state.where_items -= 1
        i = st.session_state.where_items
        for component in ['column', 'operator', 'condition', 'add', 'subtract']:
            where_component = f'where_{component}_{i}'
            if where_component in st.session_state:
                del st.session_state[where_component]
                
                
    def add_order_state():
        st.session_state.order_items += 1
        

    def subtract_order_state():
        st.session_state.order_items -= 1
        i = st.session_state.order_items
        for component in ['column', 'direction', 'add', 'subtract']:
            order_component = f'order_{component}_{i}'
            if order_component in st.session_state:
                del st.session_state[order_component]


    # Initialize number of items in where clause
    if 'where_items' not in st.session_state:
        st.session_state.where_items = 0

    # Initialize number of items in order by clause
    if 'order_items' not in st.session_state:
        st.session_state.order_items = 0


    st.write('# View Your Metrics')

    col1, col2 = st.columns(2)

    # Retrieve metrics from dictionary
    col1.multiselect(
        label='Select Metric(s)',
        options=sorted(st.session_state.metric_dict.keys()),
        default=None,
        key='selected_metrics',
        placeholder='Select a Metric'
    )

    # Retrieve unique dimensions based on overlap of metrics selected
    all_dimensions = [
        v['dimensions'] for k, v in st.session_state.metric_dict.items()
        if k in st.session_state.selected_metrics
    ]
    unique_dimensions = get_shared_elements(all_dimensions)

    if len(unique_dimensions) > 0:
        query = queries['dimensions'].format(
            **{'metrics': st.session_state.selected_metrics}
        )
        df = submit_query(st.session_state.conn, query)
        df.columns = [col.lower() for col in df.columns]
        df.set_index(keys='name', inplace=True)
        df.columns = [col.lower() for col in df.columns]
        df = df[~df.index.duplicated(keep='first')]
        st.session_state.dimension_dict = df.to_dict(orient='index')

    col2.multiselect(
        label='Select Dimension(s)',
        options=sorted(unique_dimensions),
        default=None,
        key='selected_dimensions',
        placeholder='Select a dimension'
    )

    # Only add grain if a time dimension has been selected
    if len(unique_dimensions) > 0:
        dimension_types = set([
            st.session_state.dimension_dict[dim]['type'].lower()
            for dim in st.session_state.selected_dimensions
        ])
        if 'time' in dimension_types:
            col1, col2 = st.columns(2)
            grains = [
                st.session_state.metric_dict[metric]['queryable_granularities']
                for metric in st.session_state.selected_metrics
            ]
            col1.selectbox(
                label='Select Grain',
                options=sort_by_time_length(
                    [g.strip().lower() for g in get_shared_elements(grains)]
                ),
                key='selected_grain',
            )

    # Add sections for filtering and ordering
    with st.expander('Filtering:'):
        if st.session_state.where_items == 0:
            st.button('Add Filters', on_click=add_where_state, key='static_filter_add')
        else:
            for i in range(st.session_state.where_items):
                col1, col2, col3, col4, col5 = st.columns([3, 1, 3, 1, 1])
                with col1:
                    st.selectbox(
                        label='Select Column',
                        options=sorted(unique_dimensions),
                        key=f'where_column_{i}'
                    )
                
                with col2:
                    st.selectbox(
                        label='Operator',
                        options=[
                            '=', '>', '<', '>=', '<=', '<>', 'BETWEEN', 'LIKE', 'ILIKE', 'IN', 'NOT IN'
                        ],
                        key=f'where_operator_{i}',
                    )
                
                with col3:
                    st.text_input(
                        label='Condition',
                        value='',
                        key=f'where_condition_{i}'
                    )
                
                with col4:
                    st.button('Add', on_click=add_where_state, key=f'where_add_{i}')
                
                with col5:
                    st.button(
                        'Remove',
                        on_click=subtract_where_state,
                        key=f'where_subtract_{i}',
                    )

    valid_orders = (
        st.session_state.selected_metrics + st.session_state.selected_dimensions
    )
    with st.expander('Ordering:'):
        if st.session_state.order_items == 0:
            st.button('Add Ordering', on_click=add_order_state, key='static_order_add')
        else:    
            for i in range(st.session_state.order_items):
                col1, col2, col3, col4 = st.columns([4, 2, 1, 1])
                with col1:
                    st.selectbox(
                        label='Select Column',
                        options=sorted(valid_orders),
                        key=f'order_column_{i}'
                    )
                
                with col2:
                    st.selectbox(
                        label='Operator',
                        options=['ASC', 'DESC'],
                        key=f'order_direction_{i}',
                    )
                
                with col3:
                    st.button('Add', on_click=add_order_state, key=f'order_add_{i}')
                
                with col4:
                    st.button(
                        'Remove', on_click=subtract_order_state, key=f'order_subtract_{i}'
                    )
        

    col1, col2 = st.columns(2)
    col1.number_input(
        label='Limit Rows',
        min_value=0,
        value=0,
        key='selected_limit',
        help='Limit the amount of rows returned by the query with a limit clause',
    )
    col1.caption('If set to 0, no limit will be applied')
    col2.selectbox(
        label='Explain Query',
        options=[False, True],
        key='selected_explain',
        help='Return the query from metricflow',
    )
    col2.caption('If set to true, only the generated query will be returned.')

    slq = SemanticLayerQuery(st.session_state)
    query = slq.query
    st.code(query)

    if st.button('Submit Query'):
        if len(st.session_state.selected_metrics) == 0:
            st.warning('You must select at least one metric!')
            st.stop()
        
        with st.spinner('Submitting Query...'):
            df = submit_query(st.session_state.conn, query, True)
            df.columns = [col.lower() for col in df.columns]
            st.session_state.slq = slq
            st.session_state.df = df
            st.session_state.explain = st.session_state.selected_explain

    if 'df' in st.session_state and 'slq' in st.session_state:
        if st.session_state.explain:
            st.code(st.session_state.df.iloc[0]['sql'])
        else:
            with st.expander('View Chart', expanded=True):
                create_chart(st.session_state.df, st.session_state.slq)

            with st.expander('View Data', expanded=True):
                st.dataframe(st.session_state.df, use_container_width=True)

########### FEEDBACK VIEW ###########

def feedback_view():
    
    URL = "https://api.github.com/repos/dpguthrie/dbt-sl-streamlit/issues"

    def post_issue(title: str, body: str):
        headers = {'Authorization': f'Bearer {st.secrets["GITHUB_TOKEN"]}'}
        payload = {'title': title, 'body': body}
        response = requests.post(URL, json=payload, headers=headers)
        return response


    with st.form('github_issue_form'):
        required_fields = {
            'feedback_title': 'Title',
            'feedback_description': 'Description'
        }
        st.write('Submit an Issue')
        title = st.text_input(
            label='Title', value='', key='feedback_title', placeholder='Required'
        )
        description = st.text_area(
            label='Describe Issue',
            value='',
            key='feedback_description',
            placeholder='Required'
        )
        email = st.text_input(
            label='Email Address',
            value='',
            key='feedback_email',
            placeholder='Optional',
        )
        submitted = st.form_submit_button('Submit')
        if submitted:
            for key, value in required_fields.items():
                if getattr(st.session_state, key) == '':
                    st.error(f'{value} is a required field!')
                    st.stop()
                    
            if email != '':
                description += f'Email Address: {email}\n\n{description}'
            response = post_issue(title, description)
            if response.status_code == 201:
                url = response.json()['html_url']
                st.success(f'Thanks for creating an issue.  You can view the issue [here]({url})')
            else:
                st.error(response.text)

########### RUN APP ###########
    
st.sidebar.subheader('dbt Semantic Layer')

options = ['🏠 Home', '🔭 Explore JDBC API', '🌌 Query Metrics', '👍 Feedback']

selected_page = st.sidebar.selectbox(
    label='Select Page',
    options=options,
    key='selected_page'
)

if selected_page == '🏠 Home':
    homepage_view()
elif selected_page == '🔭 Explore JDBC API':
    explore_jdbc_api_view()
elif selected_page == '🌌 Query Metrics':
    query_metrics_view()
else:
    feedback_view()
