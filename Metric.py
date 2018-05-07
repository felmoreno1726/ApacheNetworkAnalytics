import numpy as np

class Metric(object):
    '''
        Creates a new metric object
        param: network
            -> numpy double-array representing adjacency matrix
            -> i.e. network[i][j] = 1 iff nodes i and j are connected
                                = 0 else
    '''
    def __init__(self, network):
        (m, n) = network.shape
        if m != n:
            raise Exception("adjacency matrix not square")

        self.network = network
        self.size = n

    '''
        Calculates the overall clustering coefficient CL of this network
        CL = number of triangles in network / number of connected triples
    '''
    def overall_clustering_coefficient(self):
        num_triangles = 0
        num_conn_triples = 0
        for i in range(self.size):
            for j in range(i+1, self.size):
                for k in range(j+1, self.size):
                    # increment if i,j,k form a triangle (all connected to each other)
                    if self.forms_triangle(i, j, k):
                        num_triangles += 1


        for i in range(self.size):
            for j in range(self.size):
                for k in range(self.size):
                    # increment if i,j,k are a connected triple
                    if self.is_connected_triple(i, j, k):
                        num_conn_triples += 1
        # double-counted, so divide by 2
        num_conn_triples = num_conn_triples/2

        return 3.0*num_triangles/num_conn_triples

    '''
        Check if nodes i, j, and k form a complete graph (all are connected to each other)
    '''
    def forms_triangle(self, i, j, k):
        i_j_connected = self.is_connected(i, j)
        i_k_connected = self.is_connected(i, k)
        j_k_connected = self.is_connected(j, k)
        return i_j_connected and i_k_connected and j_k_connected

    '''
        Check if nodes i, j, and k form a connected triple
        - i.e. there is an edge between i and j AND an edge between j and k
        - where i, j, and k are DISTINCT nodes
    '''
    def is_connected_triple(self, i, j, k):
        # return False for non-distinct triples of nodes
        if i==j or j==k or k==i:
            return False

        i_j_connected = self.is_connected(i, j)
        j_k_connected = self.is_connected(j, k)

        return i_j_connected and j_k_connected

    '''
        Check if node i and j are connected
    '''
    def is_connected(self, i, j):
        i_to_j = self.network[i][j] != 0
        j_to_i = self.network[j][i] != 0

        # if there is an edge from node i to node j OR node j to node i,
        #   then node i and node j are connected
        return i_to_j or j_to_i


