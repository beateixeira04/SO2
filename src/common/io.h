#ifndef COMMON_IO_H
#define COMMON_IO_H

#include <stddef.h>
#include <sys/types.h>

/// Reads a given number of bytes from a file descriptor. Will block until all
/// bytes are read, or fail if not all bytes could be read.
/// @param fd File descriptor to read from.
/// @param buffer Buffer to read into.
/// @param size Number of bytes to read.
/// @param intr Pointer to a variable that will be set to 1 if the read was
/// interrupted.
/// @return On success, returns 1, on end of file, returns 0, on error, returns
/// -1
int read_all(int fd, void *buffer, size_t size, int *intr);

/// Reads a string from a file descriptor until a null character (`\0`) 
/// or newline (`\n`) is encountered, or the maximum size is reached.
/// @param fd File descriptor to read from.
/// @param str Buffer where the read string will be stored. The buffer
/// must have space for at least MAX_STRING_SIZE bytes.
/// @return Number of bytes read (excluding the null terminator) on success, 
/// or -1 on error. The resulting string will always be null-terminated.
int read_string(int fd, char *str);

/// Writes a given number of bytes to a file descriptor. Will block until all
/// bytes are written, or fail if not all bytes could be written.
/// @param fd File descriptor to write to.
/// @param buffer Buffer to write from.
/// @param size Number of bytes to write.
/// @return On success, returns 1, on error, returns -1
int safe_write(int fd, const void *buffer, size_t size);

/// Creates a named pipe (FIFO) at the specified path with the given mode.
/// If the FIFO already exists, this function does nothing and returns success.
/// @param path Path where the FIFO should be created.
/// @param mode Permissions for the FIFO (e.g., 0666 for read/write access).
/// @return 0 on success, or -1 if an error occurred 
///         (errno will be set appropriately).
int safe_mkfifo(const char *path, mode_t mode);

/// Opens a file or pipe at the specified path with the given flags.
/// Ensures error checking and reports failures appropriately.
/// @param path Path to the file or pipe to be opened.
/// @param flags Flags specifying the access mode (e.g., O_RDONLY, O_WRONLY).
/// @return File descriptor on success, or -1 if an error occurred 
///         (errno will be set appropriately).
int safe_open(const char *path, int flags);

/// Closes a file descriptor, ensuring that errors are handled properly.
/// @param fd File descriptor to be closed.
/// @return 0 on success, or -1 if an error occurred 
///         (errno will be set appropriately).
int safe_close(int fd);

/// Safely removes a file, ensuring any errors are handled properly.
/// @param path Path to the file to be unlinked.
/// @return void
void safe_unlink(const char *path);

/// Pauses the execution for a specified duration in milliseconds.
/// @param time_ms Duration to sleep in milliseconds.
/// @return void
void delay(unsigned int time_ms);

#endif // COMMON_IO_H
