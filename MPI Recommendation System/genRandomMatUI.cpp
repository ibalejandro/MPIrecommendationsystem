#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>

int main(int argc, char **argv) {
  srand (time(NULL));

  if (argc != 3) return 0;

  int movies = atoi(argv[1]);
  int users = atoi(argv[2]);

  for (int i = 0; i < movies; ++i) {
    for (int j = 0; j < users; j++) {
      int rating = rand() % 6;
      printf("%d ", rating);
    }
    printf("\n");
  }

  return 0;
}
