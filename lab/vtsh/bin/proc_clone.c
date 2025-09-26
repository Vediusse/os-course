// Запуск подпроцесса с помощью clone(2)
#define _GNU_SOURCE
#include <errno.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

typedef struct ChildContext {
  char** argv_for_exec;
  bool should_exec;
} ChildContext;

static int child_main(void* arg) {
  ChildContext* context = (ChildContext*)arg;
  if (context->should_exec) {
    execvp(context->argv_for_exec[0], context->argv_for_exec);
    
    perror("execvp");
    return 127;
  }
  const char* quiet_env = getenv("PROC_CLONE_QUIET");
  
  bool quiet = (quiet_env && strcmp(quiet_env, "0") != 0);
  if (!quiet) {
    printf("[child] pid=%ld ppid=%ld\n", (long)getpid(), (long)getppid());
    fflush(stdout);
  }
  return 0;
}

static void* allocate_stack(size_t size_bytes) {
  void* memory_block = malloc(size_bytes);
  if (memory_block == NULL)
    return NULL;
  return (uint8_t*)memory_block + size_bytes;
}

static long timespec_diff_ms(struct timespec a, struct timespec b) {
  long sec = a.tv_sec - b.tv_sec;
  long nsec = a.tv_nsec - b.tv_nsec;
  return sec * 1000 + nsec / 1000000;
}

static void print_usage(const char* program_name) {
  fprintf(stderr, "Usage: %s [--] [command [args...]]\n", program_name);
}

int main(int argc, char** argv) {
  const size_t child_stack_size = 1 << 20;
  int exit_code = 0;
  const char* quiet_env = getenv("PROC_CLONE_QUIET");
  bool quiet = (quiet_env && strcmp(quiet_env, "0") != 0);

  int first_cmd_index = 1;
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--") == 0) {
      first_cmd_index = i + 1;
      break;
    }
  }
  if (first_cmd_index == 1 && argc > 1 && argv[1][0] == '-') {
    print_usage(argv[0]);
    return 2;
  }

  bool have_command = (first_cmd_index < argc);
  ChildContext context = {
      .argv_for_exec = have_command ? &argv[first_cmd_index] : NULL,
      .should_exec = have_command,
  };

  void* child_stack_top = allocate_stack(child_stack_size);
  if (child_stack_top == NULL) {
    perror("malloc (stack)");
    return 12;
  }

  const int clone_flags = SIGCHLD;
  struct timespec t_start = {0}, t_end = {0};
  clock_gettime(CLOCK_MONOTONIC, &t_start);
  

  pid_t child_pid = clone(child_main, child_stack_top, clone_flags, &context);
  if (child_pid == -1) {
    perror("clone");
    void* stack_base_err =
        (void*)((uintptr_t)child_stack_top - child_stack_size);
    free(stack_base_err);
    return 1;
  }

  int child_status = 0;
  pid_t waited = waitpid(child_pid, &child_status, 0);
  clock_gettime(CLOCK_MONOTONIC, &t_end);

  if (waited == -1) {
    perror("waitpid");
    exit_code = 1;
  } else {
    long elapsed_ms = timespec_diff_ms(t_end, t_start);
    if (WIFEXITED(child_status)) {
      int child_exit_status = WEXITSTATUS(child_status);
      if (!quiet) {
        printf(
            "[parent] child %ld exited with %d in %ld ms\n",
            (long)child_pid,
            child_exit_status,
            elapsed_ms
        );
      }
      dprintf(
          STDERR_FILENO,
          "[time] %s %ld ms rc=%d\n",
          context.argv_for_exec ? context.argv_for_exec[0] : "<none>",
          elapsed_ms,
          child_exit_status
      );
      exit_code = child_exit_status;
    } else if (WIFSIGNALED(child_status)) {
      int sig = WTERMSIG(child_status);
      if (!quiet) {
        printf(
            "[parent] child %ld killed by signal %d in %ld ms\n",
            (long)child_pid,
            sig,
            elapsed_ms
        );
      }
      dprintf(
          STDERR_FILENO,
          "[time] %s %ld ms rc=%d (signal %d)\n",
          context.argv_for_exec ? context.argv_for_exec[0] : "<none>",
          elapsed_ms,
          128 + sig,
          sig
      );
      exit_code = 128 + sig;
    } else {
      if (!quiet) {
        printf(
            "[parent] child %ld finished in %ld ms (unknown status)\n",

            (long)child_pid,
            elapsed_ms
        );
      }
      dprintf(
          STDERR_FILENO,
          "[time] Command '%s' %ld ms rc=%d\n",
          context.argv_for_exec ? context.argv_for_exec[0] : "<none>",
          elapsed_ms,
          1
      );
      exit_code = 1;
    }
  }

  void* stack_base = (void*)((uintptr_t)child_stack_top - child_stack_size);
  free(stack_base);
  return exit_code;
}
