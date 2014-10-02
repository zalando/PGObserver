import os
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates/flot')))

graphId = 1


class Graph(object):

    def __init__(self, graphContainer=None, legendPositions=None, labelWidth=80):
        global graphId
        graphId += 1

        self.legendPositions=legendPositions
        if self.legendPositions==None:
            self.legendPositions = "left"

        self.data = {}
        self.id = graphId
        self.graphContainer = graphContainer
        self.labelWidth = labelWidth

    def render(self):
        g = env.get_template('base.html')
        return g.render(graphContainer=self.graphContainer, data=self.data.values(), legendPosition=self.legendPositions , labelWidth=self.labelWidth)

    def addPoint(self, name, x, y):
        if not ( name in self.data ):
            self.addSeries(name,name)

        self.data[name]['data'].append({'x':x,'y':y});

    def addSeries(self, label, name, color="#00ff00",data = None, yaxis = 1):
        if data == None:
            data = []

        self.data[name] = { 'label': label,
                            'name': name,
                            'color': color,
                            'data': data,
                            'yaxis': yaxis
                          }

class MultiLegendGraph( Graph ):
    def __init__(self, graphContainer=None, legendPositions=None):
        Graph.__init__(self,graphContainer,legendPositions)

    def render(self):
        g = env.get_template('baseMultiLegend.html')
        return g.render(graphContainer=self.graphContainer, data=self.data.values(), legendPosition=self.legendPositions)

class TimeGraph ( Graph ):
    def __init__(self, graphContainer=None, legendPositions=None):
        Graph.__init__(self,graphContainer,legendPositions)

    def render(self):
        g = env.get_template('data_time_graph.html')
        return g.render(graphContainer=self.graphContainer, data=self.data.values(), legendPosition=self.legendPositions)

class SizeGraph ( Graph ):
    def __init__(self, graphContainer=None, legendPositions=None,labelWidth=80):
        Graph.__init__(self,graphContainer,legendPositions,labelWidth=labelWidth)

    def render(self):
        g = env.get_template('data_size_graph.html')
        return g.render(graphContainer=self.graphContainer, data=self.data.values(), legendPosition=self.legendPositions,labelWidth=self.labelWidth)

class BarGraph ( Graph ):
    def __init__(self, graphContainer=None, legendPositions=None,labelWidth=80):
        Graph.__init__(self,graphContainer,legendPositions,labelWidth=labelWidth)

    def render(self):
        g = env.get_template('barBase.html')
        return g.render(graphContainer=self.graphContainer, data=self.data.values(), legendPosition=self.legendPositions,labelWidth=self.labelWidth)

class SelectableGraph(Graph):
    def __init__(self, graphContainer=None, choiceContainer=None):
        global graphId
        graphId += 1

        self.data = {}
        self.id = graphId
        self.graphContainer = graphContainer
        self.choiceContainer = choiceContainer

    def render(self):
        g = env.get_template('selectable.html')
        return g.render(choiceContainer=self.choiceContainer, graphContainer=self.graphContainer, data=self.data.values())
