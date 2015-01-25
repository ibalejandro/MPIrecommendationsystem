#MPI Recommendation System

This project consists in a movie's recommendation system based on the Pearson's correlation. Using a matrix of movie's ratings made by the users, the MPI Recommendation System helps to determine the most similar users and the most recommended movies for an specific user ID. Besides having been implemented in C++, the MPI standard was used for the purpose of building a distributed processing system.

The project was mainly developed using the [Open MPI] version 1.8.4 and [C++].

To generate a random matrix in order to execute and test the application a utility was developed, its name is **genRandomMatUI.cpp**, you can run it as the following example:

```sh
$ ./genRandomMatUI 100 100 matrixUI.txt
```
The command above will generate a 100 by 100 matrix.

You should compile the *cpp* file with MPI, the following hint will demonstrate how to achieve that.

```sh
$ mpic++ recommendationSystem.cpp -o recommendationSystem
```

The executable file **recommendationSystem** is the main application functionality, you can run the example by typing the following command:

```sh
$ mpirun -np 4 recommendationSystem matrixUI.txt matrixSR.txt matrixRecMovs.txt
```

After running the command, two new files will be generated, *matrixSR.txt* will contain the users who have similar preferences for each user in decreasing order. And the *matrixRecMovs.txt* contains the most recommended movies for each user.

## Querying

In order to query the results anther utility program was developed, that program searches for each user the most recommended movies.
In the following example the user 2 is being queried by its most recommended movies:
```sh
$ recommendedMovies matrixRecMovs.txt 2
```
If the user has recommended movies they will be shown, otherwise, a message will notify that the user doesn't have any recommended movie.

[Open MPI]:http://www.open-mpi.org/
[C++]:http://www.cplusplus.com/
