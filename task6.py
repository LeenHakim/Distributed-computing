from mrjob.job import MRJob
from mrjob.job import MRStep
import numpy as np
import os

input_file = open('C:\Pycharm\DC_project\A.txt',  'r+')
linesOfFile = input_file.readlines()

input_file2 = open('C:\Pycharm\DC_project\B.txt',  'r+')
linesOfFile2 = input_file2.readlines()

noOfRows = len(linesOfFile)
column_first = len(linesOfFile[0].split())
rows_second = len(linesOfFile2)
noOfCols = len(linesOfFile2[0].split())

row_counter_A = 0
row_counter_B = 0


class MatrixMultiplication(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_dot_products,
                   reducer=self.reducer_merge_elements_from_A_B),
            MRStep(mapper=self.mapper_perform_dot_product)
        ]

    def mapper_extract_dot_products(self, _, line):
        """
        This mapper reads each row of matrix A and matrix B. for matrix A it produce (key,values) like follows:
        (key, value)=((i, k), (A, j, Aij)) for all k
        and for matrix B it produces (key,value) as follows:
        (key, value)=((i, k), (B, j, Bjk)) for all i
        :param _:
        :param line: each row of input matrices
        :return:   (i,k)(A,j,Aij) for matrix A  |||  (i,k)(B,j,Bjk)  for matrix B
        """
        global row_counter_A
        global row_counter_B
        line_array = line.split()
        filename = os.environ['map_input_file']
        if filename == 'file://C:\Pycharm\DC_project\A.txt':
            row_counter_A += 1
            for column_counter_A in range(1, noOfCols + 1):
                for x in range(1, len(line_array) + 1):
                    yield (row_counter_A, column_counter_A), ('A', x, float(line_array[x - 1]))

        if filename == 'file://C:\Pycharm\DC_project\B.txt':
            row_counter_B += 1
            for row_counter in range(1, noOfRows + 1):
                for x in range(1, len(line_array) + 1):
                    yield (row_counter, x), ('B', row_counter_B, float(line_array[x - 1]))

    def reducer_merge_elements_from_A_B(self, position_index, values_in_matrix):
        """
        this reducer will gather all the values for each key. each key represents the position index in result matrix.
        :param position_index:  the position of matrix like (1,1),(1,2)(2,1)(2,2) in square matrix
        :param values_in_matrix:
        :return: key: position in the matrix value: elements in matrix A and B that should be multiplied to
        fill that position in result matrix
        """
        yield position_index, sorted(values_in_matrix)

    def mapper_perform_dot_product(self, matrix_index, value):
        """
        This function does the final job. for each key which represents the index in matrix, it does the sum product
        between the corresponding elements in A and B
        :param matrix_index: index (i,k) in result  matrix
        :param value: final value for each index
        :return: index,value of result matrix in corresponding index
        """
        final_result = 0
        for x in range(0, column_first):
            temp_result = 1
            for i in range(x, 2 * column_first, column_first):
                temp_result *= value[i][2]
            final_result += temp_result
        yield matrix_index, final_result


"""
in final result we have keys and values. the main purpose of reshape() function is to transform the result in 
matrix shape. the only parameter "filepath" is the path of the result matrix from MapReduce procedure. 
"""


def reshape():
    filepath = 'C:\\Pycharm\\DC_project\\result.txt'
    matrix = np.zeros((noOfRows, noOfCols))
    file = open(filepath,
                'r+')
    lines = file.readlines()
    for i in range(0, len(lines)):
        lines[i] = lines[i].split()

    bad_chars = ['[', ']', ',']
    for x in lines:
        for i in range(0, len(x)):
            for xx in bad_chars:
                x[i] = x[i].replace(xx, '')
    for x in lines:
        row = int(x[0]) - 1
        column = int(x[1]) - 1
        value = x[2]
        matrix[row][column] = value

    np.savetxt('C:\\Pycharm\\DC_project\\matrixxx.txt', matrix)


if __name__ == '__main__':
    MatrixMultiplication.run()
    reshape()
