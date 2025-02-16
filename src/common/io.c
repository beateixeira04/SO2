#include "io.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "src/common/constants.h"

int read_all(int fd, void *buffer, size_t size, int *intr) {
  if (intr != NULL && *intr) {
    return -1;
  }
  size_t bytes_read = 0;
  while (bytes_read < size) {
    ssize_t result = read(fd, buffer + bytes_read, size - bytes_read);
    if (result == -1) {
      if (errno == EINTR) {
        if (intr != NULL) {
          *intr = 1;
          if (bytes_read == 0) {
            return -1;
          }
        }
        continue;
      }
      perror("Failed to read from pipe");
      return -1;
    } else if (result == 0) {
      return 0;
    }
    bytes_read += (size_t)result;
  }
  return 1;
}

int read_string(int fd, char *str) {
  ssize_t bytes_read = 0;
  char ch;
  while (bytes_read < MAX_STRING_SIZE - 1) {
    if (read(fd, &ch, 1) != 1) {
      return -1;
    }
    if (ch == '\0' || ch == '\n') {
      break;
    }
    str[bytes_read++] = ch;
  }
  str[bytes_read] = '\0';
  return (int)bytes_read;
}

int safe_write(int fd, const void *buf, size_t size) {
  do {
    ssize_t bytes_written = write(fd, buf, size);
    if (bytes_written == -1) {
      if (errno == EPIPE) {
        return 2;
      } else if (errno != EINTR) {
        fprintf(stderr, "[ERR]: write failed: %s\n", strerror(errno));
        return 1;
      }
    }

    size -= (size_t)bytes_written;
  } while (size > 0);

  return 0;
}

static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

void delay(unsigned int time_ms) {
  struct timespec delay = delay_to_timespec(time_ms);
  nanosleep(&delay, NULL);
}

int safe_open(const char *pathname, int flags) {
  int fd = open(pathname, flags);
  if (fd == -1) {
    fprintf(stderr, "[Err]: open failed: %s\n", strerror(errno));
  }

  return fd;
}
int safe_close(int fd) {
  if (close(fd) == -1) {
    fprintf(stderr, "[Err]: close failed: %s\n", strerror(errno));
    return 1;
  }
  return 0;
}
int safe_mkfifo(const char *fifo_name, mode_t mode) {
  safe_unlink(fifo_name);
  if (mkfifo(fifo_name, mode) != 0) {
    fprintf(stderr, "mkfifo failed: %s\n", strerror(errno));
    return 1;
  }
  return 0;
}

void safe_unlink(const char *pathname) {
  if (unlink(pathname) != 0 && errno != ENOENT) {
    fprintf(stderr, "unlink(%s) failed: %s\n", pathname, strerror(errno));
  }
}
