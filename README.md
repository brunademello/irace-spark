# Automatic configuration of Spark parameters using Irace

## Introduction

The Apache Spark is a set of open source tools widely used for distributed processing of large volume of data. It has more than 180 parameters with their respective domains which can be configurated when an application starts. The most appropriate choice of value for each parameter allows optimizing code execution, decreasing the processing costs which is associated to availability of computing resources. However the right choice of parameters is hard to do manually due to high number of possible combinations, which can be a limitation in the search for the best set of parameters.

The irace package is a algorithm that implements the iterated racing and it is used for optimize an application by chosing the best parameters configuration automatically. Given a set of parameters and a range of values from their domains, the irace execute the target runner several times in order to find the best combination considering the cost definied in the application. 

![Irace](images/irace-flow.jpg)


## Features

## Installation

## Usage

### wordcount app