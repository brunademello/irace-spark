from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import plotly.graph_objs as go
import dash_table
from base64 import b64encode
import io

buffer = io.StringIO()

html_bytes = buffer.getvalue().encode()
encoded = b64encode(html_bytes).decode()

data = {
    'config': ['20240328', '20240330', '20240330_only_spark', '20240330_only_cassandra', '20230402', '20230402_only_spark', '20230402_instances_default'],
    'configuration_1': [16.0153562, 16.109491, 31.2413048, 16.763625400000002, 16.0631024, 31.3581946, 16.1011728],
    'configuration_2': [16.142181, 16.0318159, 31.6775965, 16.9564536, 16.188971900000002, 31.0322206, 16.1380133],
    'configuration_3': [15.852506199999999, 16.041157899999998, 31.5365796, 16.7198404, 16.1448714, 31.3146187, 16.292319],
    'configuration_4': [16.1629656, 15.8409186, 31.5477953, 16.7571328, 16.100718, 31.341838799999998, 16.3846836],
    'configuration_5': [15.852838700000001, 15.869535099999998, 0, 0, 16.241861, 31.159679699999998, 16.3562929],
    'default': [31.222159499999997, 31.022122200000002, 31.7906497, 31.540408000000003, 31.4165415, 31.159679699999998, 31.3841163]
}

df = pd.DataFrame(data)
melted_df = pd.melt(df, id_vars=['config'], var_name='experiment', value_name='value')

app = Dash(__name__)

app.layout = html.Div([
    html.H1(children='Experimentos - Irace Spark', style={'textAlign':'center', 
                                                          'font-family': "verdana"}
                                                          ),
    dcc.Dropdown(melted_df.config.unique(), '20240330', style={'font-family': 'verdana'}, id='dropdown-selection'),
    dcc.Graph(id='graph-content'),
])

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    dff = melted_df[melted_df.config==value]
    fig = px.bar(dff, x='experiment', y='value')
    fig.update_traces(marker_color='rgb(165, 165, 165)')
    fig.update_traces(text=dff['value'].values.flatten(), textposition='outside')
    fig.update_layout(title='Média de tempo por configuração', xaxis_title='Configuração', yaxis_title='Tempo (segundos)')

    return fig

if __name__ == '__main__':
    app.run(debug=True)
