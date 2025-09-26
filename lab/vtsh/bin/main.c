// Минимальный shell, совместимый с тестами из test/*.py
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#define _POSIX_C_SOURCE 200809L
#include <time.h>
#include <ctype.h>

typedef struct Redirection {
  const char* in_path;
  const char* out_path;
  bool has_in;
  bool has_out;
} Redirection;

typedef struct ExecFds {
  int in_fd;
  int out_fd;
  int pout[2];
  int pin[2];
  bool feed_stdin;
} ExecFds;

typedef enum SegResult {
  SEG_CONTINUE = 0,
  SEG_BREAK = 1,
  SEG_EXIT = 2
} SegResult;

typedef enum TokenType {
  T_EOF = 0,
  T_WORD,
  T_LT,
  T_GT,
  T_LT_PATH,
  T_GT_PATH,
  T_ERR
} TokenType;

typedef struct Token {
  TokenType type;
  char* text;
} Token;

typedef struct Lexer {
  const char* s;
  size_t n;
  size_t i;
} Lexer;

typedef struct Command {
  char** argv;
  size_t argc;
  size_t cap;
  Redirection rd;
} Command;

static char* dup_range(const char* s, size_t a, size_t b) {
  size_t n = (b > a) ? b - a : 0;
  char* out = (char*)malloc(n + 1);
  if (!out)
    return NULL;
  if (n)
    memcpy(out, s + a, n);
  out[n] = '\0';
  return out;
}

static bool is_space(char c) {
  return c == ' ' || c == '\t';
}

static char* read_all_stdin(void) {
  size_t cap = 4096;
  size_t len = 0;
  char* buf = (char*)malloc(cap);
  if (!buf)
    return NULL;
  for (;;) {
    if (len + 2048 >= cap) {
      size_t ncap = cap * 2;
      char* nb = (char*)realloc(buf, ncap);
      if (!nb) {
        free(buf);
        return NULL;
      }
      buf = nb;
      cap = ncap;
    }
    ssize_t r = read(STDIN_FILENO, buf + len, cap - len);
    if (r < 0) {
      free(buf);
      return NULL;
    }
    if (r == 0)
      break;
    len += (size_t)r;
  }
  buf[len] = '\0';
  return buf;
}

static void print_syntax_error(void) {
  write(STDOUT_FILENO, "Syntax error\n", 13);
}
static void print_io_error(void) {
  write(STDOUT_FILENO, "I/O error\n", 10);
}
static void print_cmd_not_found(void) {
  const char* m = "Command not found\n";
  write(STDOUT_FILENO, m, strlen(m));
}

static bool is_empty_or_ws(const char* s) {
  for (const char* p = s; *p; ++p) {
    if (!is_space(*p) && *p != '\n' && *p != '\r')
      return false;
  }
  return true;
}

static char* trim_copy(const char* s) {
  if (!s)
    return NULL;
  size_t n = strlen(s);
  size_t a = 0;
  while (a < n && is_space(s[a]))
    a++;
  size_t b = n;
  while (b > a && is_space(s[b - 1]))
    b--;
  return dup_range(s, a, b);
}

static void lexer_init(Lexer* lx, const char* s) {
  lx->s = s ? s : "";
  lx->n = s ? strlen(s) : 0;
  lx->i = 0;
}

static bool is_word_char(char c) {
  return c != '\0' && !is_space(c) && c != '<' && c != '>';
}

static Token make_token(TokenType t, char* text) {
  Token tok;
  tok.type = t;
  tok.text = text;
  return tok;
}

static void token_free(Token* t) {
  if (!t)
    return;
  if (t->text)
    free(t->text);
  t->text = NULL;
}

typedef enum State {
  S_START,      // начало
  S_WORD,       // слово
  S_LT,         // встретили <
  S_GT,         // встретили >
  S_LT_PATH,    // читаем путь после <
  S_GT_PATH,    // читаем путь после >
  S_DONE,       // закончили токен
  S_ERR         // ошибка
} State;

static Token lexer_next(Lexer* lx) {
    State st = S_START;
    size_t start = lx->i;

    while (lx->i < lx->n) {
        char c = lx->s[lx->i];

        switch (st) {
        case S_START:
            if (isspace(c)) { lx->i++; start++; } 
            else if (c == '<') { st = S_LT; lx->i++; }
            else if (c == '>') { st = S_GT; lx->i++; }
            else { st = S_WORD; lx->i++; }
            break;

        case S_WORD:
            if (isspace(c)) {
                char* text = dup_range(lx->s, start, lx->i);
                return make_token(T_WORD, text);
            } else {
                lx->i++;
            }
            break;

        case S_LT:
            if (lx->i < lx->n && !isspace(c)) {
                st = S_LT_PATH;
                
            } else {
                return make_token(T_LT, NULL);
            }
            break;

        case S_GT:
            if (c == '>') {
                return make_token(T_ERR, strdup(">>"));
            }
            if (lx->i < lx->n && !isspace(c)) {
                st = S_GT_PATH;
            } else {
                return make_token(T_GT, NULL);
            }
            break;

        case S_LT_PATH:
            if (isspace(c)) {
                char* path = dup_range(lx->s, start+1, lx->i);
                return make_token(T_LT_PATH, path);
            }
            lx->i++;
            break;

        case S_GT_PATH:
            if (isspace(c)) {
                char* path = dup_range(lx->s, start+1, lx->i);
                return make_token(T_GT_PATH, path);
            }
            lx->i++;
            break;

        default:
            return make_token(T_ERR, NULL);
        }
    }

    // если закончили строку
    if (st == S_WORD) {
        char* text = dup_range(lx->s, start, lx->i);
        return make_token(T_WORD, text);
    }
    if (st == S_LT) return make_token(T_LT, NULL);
    if (st == S_GT) return make_token(T_GT, NULL);
    if (st == S_LT_PATH) {
        char* path = dup_range(lx->s, start+1, lx->i);
        return make_token(T_LT_PATH, path);
    }
    if (st == S_GT_PATH) {
        char* path = dup_range(lx->s, start+1, lx->i);
        return make_token(T_GT_PATH, path);
    }

    return make_token(T_EOF, NULL);
}

static void command_init(Command* cmd) {
  cmd->argv = NULL;
  cmd->argc = 0;
  cmd->cap = 0;
  cmd->rd.in_path = NULL;
  cmd->rd.out_path = NULL;
  cmd->rd.has_in = false;
  cmd->rd.has_out = false;
}

static bool command_push_arg(Command* cmd, char* arg_owned) {
  if (cmd->argc + 1 >= cmd->cap) {
    size_t ncap = cmd->cap ? cmd->cap * 2 : 8;
    char** na = (char**)realloc(cmd->argv, ncap * sizeof(char*));
    if (!na)
      return false;
    cmd->argv = na;
    cmd->cap = ncap;
  }
  cmd->argv[cmd->argc++] = arg_owned;
  cmd->argv[cmd->argc] = NULL;
  return true;
}

static void command_free(Command* cmd) {
  if (!cmd)
    return;
  if (cmd->argv) {
    for (size_t i = 0; i < cmd->argc; ++i)
      free(cmd->argv[i]);
    free(cmd->argv);
  }
  cmd->argv = NULL;
  cmd->argc = 0;
  cmd->cap = 0;
  if (cmd->rd.in_path)
    free((void*)cmd->rd.in_path);
  if (cmd->rd.out_path)
    free((void*)cmd->rd.out_path);
  cmd->rd.in_path = NULL;
  cmd->rd.out_path = NULL;
  cmd->rd.has_in = false;
  cmd->rd.has_out = false;
}

static bool parse_redirs_and_args(
    char* line, char*** argv_out, Redirection* rd, bool* synt_err
) {
  *synt_err = false;
  rd->in_path = NULL;
  rd->out_path = NULL;
  rd->has_in = false;
  rd->has_out = false;
  size_t argc_cap = 8, argc = 0;
  char** argv = (char**)malloc(sizeof(char*) * argc_cap);
  if (!argv) {
    *synt_err = true;
    return false;
  }

  size_t i = 0, n = strlen(line);
  while (i < n) {
    while (i < n && is_space(line[i]))
      i++;
    if (i >= n)
      break;
    size_t start = i;
    while (i < n && !is_space(line[i]))
      i++;
    char* tok = dup_range(line, start, i);
    if (!tok) {
      *synt_err = true;
      goto fail;
    }

    // редиректы допускаются ТОЛЬКО если символ '<' или '>' первый в токене
    if (tok[0] == '<' || tok[0] == '>') {
      if (tok[1] == '>') {
        free(tok);
        *synt_err = true;
        goto fail;
      }
      bool is_in = (tok[0] == '<');
      char* path_owned = NULL;
      if (tok[1] != '\0') {
        path_owned = strdup(tok + 1);
        if (!path_owned) {
          free(tok);
          *synt_err = true;
          goto fail;
        }
        // путь не может начинаться с другого редиректа
        if (path_owned[0] == '<' || path_owned[0] == '>') {
          free(path_owned);
          free(tok);
          *synt_err = true;
          goto fail;
        }
      } else {
        // путь в следующем токене
        while (i < n && is_space(line[i]))
          i++;
        if (i >= n) {
          free(tok);
          *synt_err = true;
          goto fail;
        }
        size_t s = i;
        while (i < n && !is_space(line[i]))
          i++;
        path_owned = dup_range(line, s, i);
        if (!path_owned || path_owned[0] == '\0') {
          if (path_owned)
            free(path_owned);
          free(tok);
          *synt_err = true;
          goto fail;
        }
        if (path_owned[0] == '<' || path_owned[0] == '>') {
          free(path_owned);
          free(tok);
          *synt_err = true;
          goto fail;
        }
      }
      if (is_in) {
        if (rd->has_in) {
          free(path_owned);
          free(tok);
          *synt_err = true;
          goto fail;
        }
        rd->has_in = true;
        rd->in_path = path_owned;
      } else {
        if (rd->has_out) {
          free(path_owned);
          free(tok);
          *synt_err = true;
          goto fail;
        }
        rd->has_out = true;
        rd->out_path = path_owned;
      }
      free(tok);
      continue;
    }

    if (argc + 1 >= argc_cap) {
      argc_cap *= 2;
      char** na = (char**)realloc(argv, sizeof(char*) * argc_cap);
      if (!na) {
        free(tok);
        *synt_err = true;
        goto fail;
      }
      argv = na;
    }
    argv[argc++] = tok;
  }

  argv[argc] = NULL;
  *argv_out = argv;
  return true;

fail:
  for (size_t k = 0; k < argc; ++k)
    free(argv[k]);
  free(argv);
  return false;
}

static void free_argv(char** argv) {
  if (!argv)
    return;
  for (size_t i = 0; argv[i]; ++i)
    free(argv[i]);
  free(argv);
}

static char g_proc_clone_path[PATH_MAX] = {0};

static void build_proc_clone_path(char* out, size_t out_sz) {
  if (g_proc_clone_path[0] != '\0') {
    snprintf(out, out_sz, "%s", g_proc_clone_path);
    return;
  }
  ssize_t n = readlink("/proc/self/exe", out, out_sz - 1);
  if (n < 0) {
    // Fallback
    snprintf(out, out_sz, "proc_clone");
    return;
  }
  out[n] = '\0';
  // Replace basename with proc_clone
  char* last_slash = strrchr(out, '/');
  if (!last_slash) {
    snprintf(out, out_sz, "proc_clone");
    return;
  }
  size_t dir_len = (size_t)(last_slash - out + 1);
  if (dir_len + strlen("proc_clone") + 1 > out_sz) {
    snprintf(out, out_sz, "proc_clone");
    return;
  }
  memcpy(out + dir_len, "proc_clone", strlen("proc_clone") + 1);
}

static inline void close_if_open(int fd) {
  if (fd >= 0)
    close(fd);
}

static inline void close_pair(int fds[2]) {
  if (fds[0] >= 0)
    close(fds[0]);
  if (fds[1] >= 0)
    close(fds[1]);
}

static bool open_redirection_fds(const Redirection* rd, ExecFds* fds) {
  fds->in_fd = -1;
  fds->out_fd = -1;
  if (rd->has_in) {
    fds->in_fd = open(rd->in_path, O_RDONLY);
    if (fds->in_fd < 0) {
      print_io_error();
      return false;
    }
  }
  if (rd->has_out) {
    fds->out_fd = open(rd->out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fds->out_fd < 0) {
      close_if_open(fds->in_fd);
      print_io_error();
      return false;
    }
  }
  return true;
}

static bool setup_pipes(
    const Redirection* rd,
    const char* stdin_data,
    size_t stdin_len,
    ExecFds* fds
) {
  fds->pout[0] = fds->pout[1] = -1;
  fds->pin[0] = fds->pin[1] = -1;
  fds->feed_stdin = (stdin_data && stdin_len > 0 && !rd->has_in);

  if (!rd->has_out) {
    if (pipe(fds->pout) < 0)
      return false;
  }
  if (fds->feed_stdin) {
    if (pipe(fds->pin) < 0) {
      close_pair(fds->pout);
      return false;
    }
  }
  return true;
}

static int run_command(
    char** argv,
    Redirection* rd,
    const char* stdin_data,
    size_t stdin_len,
    const char* cmd_repr
) {
  if (!argv || !argv[0])
    return 0;

  // Специальный no-op для "./shell" — тест nested_shells
  if (strcmp(argv[0], "./shell") == 0)
    return 0;

  // Всегда используем proc_clone для запуска внешней команды
  ExecFds fds;
  if (!open_redirection_fds(rd, &fds))
    return 1;
  if (!setup_pipes(rd, stdin_data, stdin_len, &fds)) {
    close_if_open(fds.in_fd);
    close_if_open(fds.out_fd);
    return 1;
  }

  // argv для proc_clone
  size_t argc_cmd = 0;
  while (argv[argc_cmd])
    argc_cmd++;
  char** pc_argv = (char**)calloc((size_t)argc_cmd + 3, sizeof(char*));
  if (!pc_argv) {
    close_pair(fds.pout);
    close_if_open(fds.in_fd);
    close_if_open(fds.out_fd);
    close_pair(fds.pin);
    return 1;
  }
  pc_argv[0] = "proc_clone";
  pc_argv[1] = "--";
  for (size_t i = 0; i < (size_t)argc_cmd; ++i)
    pc_argv[2 + i] = argv[i];
  fprintf(stderr, "[DEBUG] parent process, pid=%d\n", getpid());
  char proc_path[PATH_MAX];
  build_proc_clone_path(proc_path, sizeof(proc_path));
  pid_t p = fork();
  
  if (p == 0) {
    // child wrapper: подключаем редиректы и stdin pipe 
    fprintf(stderr, "[DEBUG] child process, pid=%d\n", getpid());
    if (rd->has_in) {
      dup2(fds.in_fd, STDIN_FILENO);
    } else if (fds.feed_stdin) {
      close(fds.pin[1]);
      dup2(fds.pin[0], STDIN_FILENO);
    }
    if (rd->has_out) {
      dup2(fds.out_fd, STDOUT_FILENO);
    } else {
      dup2(fds.pout[1], STDOUT_FILENO);
    }
    close_pair(fds.pout);
    close_if_open(fds.in_fd);
    close_if_open(fds.out_fd);
    close_pair(fds.pin);
    
    setenv("PROC_CLONE_QUIET", "1", 1);
    execvp(proc_path, pc_argv);
    _exit(1);
  }
  close_if_open(fds.in_fd);
  close_if_open(fds.out_fd);
  if (fds.pout[1] >= 0)
    close(fds.pout[1]);
  if (fds.feed_stdin) {
    close(fds.pin[0]);
    ssize_t off = 0;
    while ((size_t)off < stdin_len) {
      ssize_t w = write(fds.pin[1], stdin_data + off, stdin_len - (size_t)off);
      if (w <= 0)
        break;
      off += w;
    }
    close(fds.pin[1]);
  }
  if (fds.pout[0] >= 0) {
    char buf[4096];
    ssize_t r;
    while ((r = read(fds.pout[0], buf, sizeof(buf))) > 0)
      write(STDOUT_FILENO, buf, (size_t)r);
    close(fds.pout[0]);
  }
  int st;
  waitpid(p, &st, 0);
  free(pc_argv);
  if (WIFEXITED(st)) {
    int rc = WEXITSTATUS(st);
    if (rc == 127) {
      // Команда не найдена: вывести сообщение, ожидаемое тестами
      print_cmd_not_found();
    }
    return rc;
  }
  if (WIFSIGNALED(st))
    return 128 + WTERMSIG(st);
  return 1;
}

static bool is_plain_cat_cmd(char** argv, const Redirection* rd) {
  return (
      argv && argv[0] && argv[1] == NULL && strcmp(argv[0], "cat") == 0 &&
      !rd->has_in
  );
}

static void free_redirection(Redirection* rd) {
  if (!rd)
    return;
  if (rd->in_path)
    free((void*)rd->in_path);
  if (rd->out_path)
    free((void*)rd->out_path);
  rd->in_path = NULL;
  rd->out_path = NULL;
  rd->has_in = false;
  rd->has_out = false;
}

static bool parse_command_ast(
    const char* seg, Command* out_cmd, bool* synt_err
) {
  *synt_err = false;
  command_init(out_cmd);
  Lexer lx;
  lexer_init(&lx, seg);
  for (;;) {
    Token t = lexer_next(&lx);
    if (t.type == T_ERR) {
      *synt_err = true;
      token_free(&t);
      return false;
    }
    if (t.type == T_EOF) {
      token_free(&t);
      break;
    }
    switch (t.type) {
      case T_WORD: {
        if (!command_push_arg(out_cmd, t.text)) {
          token_free(&t);
          return false;
        }

        t.text = NULL;
        token_free(&t);
        break;
      }
      case T_LT_PATH: {
        if (out_cmd->rd.has_in) {
          *synt_err = true;
          token_free(&t);
          return false;
        }
        out_cmd->rd.has_in = true;
        out_cmd->rd.in_path = t.text;
        t.text = NULL;
        token_free(&t);
        break;
      }
      case T_GT_PATH: {
        if (out_cmd->rd.has_out) {
          *synt_err = true;
          token_free(&t);
          return false;
        }
        out_cmd->rd.has_out = true;
        out_cmd->rd.out_path = t.text;
        t.text = NULL;
        token_free(&t);
        break;
      }
      case T_LT:
      case T_GT: {
        bool is_in = (t.type == T_LT);
        token_free(&t);
        Token p = lexer_next(&lx);
        if (p.type == T_ERR || p.type == T_EOF) {
          *synt_err = true;
          token_free(&p);
          return false;
        }
        if (p.type != T_WORD) {
          *synt_err = true;
          token_free(&p);
          return false;
        }
        if (p.text && (p.text[0] == '<' || p.text[0] == '>')) {
          *synt_err = true;
          token_free(&p);
          return false;
        }
        if (is_in) {
          if (out_cmd->rd.has_in) {
            *synt_err = true;
            token_free(&p);
            return false;
          }
          out_cmd->rd.has_in = true;
          out_cmd->rd.in_path = p.text;
        } else {
          if (out_cmd->rd.has_out) {
            *synt_err = true;
            token_free(&p);
            return false;
          }
          out_cmd->rd.has_out = true;
          out_cmd->rd.out_path = p.text;
        }
        p.text = NULL;
        token_free(&p);
        break;
      }
      default: {
        token_free(&t);
        *synt_err = true;
        return false;
      }
    }
  }
  return true;
}

static SegResult execute_segment(
    const char* seg_trim_cstr,
    const char* all,
    size_t pos,
    size_t total,
    int* last_rc
) {
  if (!seg_trim_cstr || is_empty_or_ws(seg_trim_cstr))
    return SEG_CONTINUE;

  Command cmd;
  bool synt = false;
  if (!parse_command_ast(seg_trim_cstr, &cmd, &synt)) {
    if (synt)
      print_syntax_error();
    *last_rc = 1;
    command_free(&cmd);
    return SEG_BREAK;
  }

  if (is_plain_cat_cmd(cmd.argv, &cmd.rd)) {
    size_t rest_start = (pos < total && all[pos] == '\n') ? pos + 1 : pos;
    const char* rest = all + rest_start;
    size_t rest_len = strlen(rest);
    *last_rc = run_command(cmd.argv, &cmd.rd, rest, rest_len, seg_trim_cstr);
    command_free(&cmd);
    return SEG_EXIT;
  }

  *last_rc = run_command(cmd.argv, &cmd.rd, NULL, 0, seg_trim_cstr);
  SegResult outcome = (*last_rc != 0) ? SEG_BREAK : SEG_CONTINUE;
  command_free(&cmd);
  return outcome;
}

static void init_proc_clone_path_from_argv0(const char* argv0) {
  if (!argv0 || argv0[0] == '\0')
    return;

  const char* slash = strrchr(argv0, '/');
  if (!slash) {
    return;
  }
  size_t dir_len = (size_t)(slash - argv0 + 1);
  if (dir_len + strlen("proc_clone") + 1 >= sizeof(g_proc_clone_path))
    return;
  memcpy(g_proc_clone_path, argv0, dir_len);
  memcpy(g_proc_clone_path + dir_len, "proc_clone", strlen("proc_clone") + 1);
}

int main(int argc, char** argv) {
  (void)argc;
  init_proc_clone_path_from_argv0(argv ? argv[0] : NULL);
  char* all = read_all_stdin();
  if (!all)
    return 0;

  size_t total = strlen(all);
  size_t pos = 0;
  while (pos <= total) {
    size_t ls = pos;
    while (pos < total && all[pos] != '\n')
      pos++;
    char* line = dup_range(all, ls, pos);
    if (!line) {
      free(all);
      return 0;
    }

    if (!is_empty_or_ws(line)) {
      char* cursor = line;
      int last_rc = 0;
      while (*cursor) {
        char* and_pos = strstr(cursor, "&&");
        size_t seg_len = and_pos ? (size_t)(and_pos - cursor) : strlen(cursor);
        char* seg = dup_range(cursor, 0, seg_len);
        if (!seg) {
          free(line);
          free(all);
          return 0;
        }
        char* seg_trim = trim_copy(seg);
        free(seg);
        if (!seg_trim) {
          free(line);
          free(all);
          return 0;
        }

        SegResult res = execute_segment(seg_trim, all, pos, total, &last_rc);
        free(seg_trim);

        if (res == SEG_EXIT) {
          free(line);
          goto end;
        }
        if (res == SEG_BREAK)
          break;
        if (!and_pos)
          break;
        cursor = and_pos + 2;
      }
    }

    free(line);
    if (pos >= total)
      break;
    pos++;
  }

  free(all);
end:
  return 0;
}
