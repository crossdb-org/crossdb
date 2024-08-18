/* crossline.h -- Version 1.0
 *
 * Crossline is a small, self-contained, zero-config, MIT licensed,
 *   cross-platform, readline and libedit replacement.
 *
 * You can find the latest source code and description at:
 *   https://github.com/jcwangxp/crossline
 *
 * ------------------------------------------------------------------------
 *
 * MIT License
 *
 * Copyright (c) 2019, JC Wang (wang_junchuan@163.com)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * ------------------------------------------------------------------------
 */

#ifndef __CROSSLINE_H
#define __CROSSLINE_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef CLN_EXPORT
#define CLN_EXPORT extern
#endif

typedef enum {
	CROSSLINE_FGCOLOR_DEFAULT       = 0x00,
	CROSSLINE_FGCOLOR_BLACK         = 0x01,
	CROSSLINE_FGCOLOR_RED           = 0x02,
	CROSSLINE_FGCOLOR_GREEN         = 0x03,
	CROSSLINE_FGCOLOR_YELLOW        = 0x04,
	CROSSLINE_FGCOLOR_BLUE          = 0x05,
	CROSSLINE_FGCOLOR_MAGENTA       = 0x06,
	CROSSLINE_FGCOLOR_CYAN          = 0x07,
	CROSSLINE_FGCOLOR_WHITE     	= 0x08,
	CROSSLINE_FGCOLOR_BRIGHT     	= 0x80,
	CROSSLINE_FGCOLOR_MASK     	    = 0x7F,

	CROSSLINE_BGCOLOR_DEFAULT       = 0x0000,
	CROSSLINE_BGCOLOR_BLACK         = 0x0100,
	CROSSLINE_BGCOLOR_RED           = 0x0200,
	CROSSLINE_BGCOLOR_GREEN         = 0x0300,
	CROSSLINE_BGCOLOR_YELLOW        = 0x0400,
	CROSSLINE_BGCOLOR_BLUE          = 0x0500,
	CROSSLINE_BGCOLOR_MAGENTA       = 0x0600,
	CROSSLINE_BGCOLOR_CYAN          = 0x0700,
	CROSSLINE_BGCOLOR_WHITE     	= 0x0800,
	CROSSLINE_BGCOLOR_BRIGHT     	= 0x8000,
	CROSSLINE_BGCOLOR_MASK     	    = 0x7F00,

	CROSSLINE_UNDERLINE     	    = 0x10000,

	CROSSLINE_COLOR_DEFAULT         = CROSSLINE_FGCOLOR_DEFAULT | CROSSLINE_BGCOLOR_DEFAULT
} crossline_color_e;

// Main API to read a line, return buf if get line, return NULL if EOF.
CLN_EXPORT char* crossline_readline (const char *prompt, char *buf, int size);

// Same with crossline_readline except buf holding initial input for editing.
//CLN_EXPORT char* crossline_readline2 (const char *prompt, char *buf, int size);

// Set move/cut word delimiter, default is all not digital and alphabetic characters.
//CLN_EXPORT void  crossline_delimiter_set (const char *delim);

// Read a character from terminal without echo
CLN_EXPORT int	 crossline_getch (void);


/* 
 * History APIs
 */

// Save history to file
//CLN_EXPORT int   crossline_history_save (const char *filename);

// Load history from file
//CLN_EXPORT int   crossline_history_load (const char *filename);

// Show history in buffer
CLN_EXPORT void  crossline_history_show (void);

// Clear history
CLN_EXPORT void  crossline_history_clear (void);


/*
 * Completion APIs
 */

typedef struct crossline_completions_t crossline_completions_t;
typedef void (*crossline_completion_callback) (const char *buf, crossline_completions_t *pCompletions);
typedef void (*crossline_completion_callback2) (const char *buf, crossline_completions_t *pCompletions, void *pArg);

// Register completion callback
#define crossline_completion_register(pCbFunc)	crossline_completion_register2((crossline_completion_callback2)pCbFunc, NULL)
CLN_EXPORT void  crossline_completion_register2 (crossline_completion_callback2 pCbFunc, void *pArg);

// Add completion in callback. Word is must, help for word is optional.
CLN_EXPORT void  crossline_completion_add (crossline_completions_t *pCompletions, const char *word, const char *help);

// Add completion with color.
CLN_EXPORT void  crossline_completion_add_color (crossline_completions_t *pCompletions, const char *word, 
														crossline_color_e wcolor, const char *help, crossline_color_e hcolor);

// Set syntax hints in callback
//CLN_EXPORT void  crossline_hints_set (crossline_completions_t *pCompletions, const char *hints);

// Set syntax hints with color
CLN_EXPORT void  crossline_hints_set_color (crossline_completions_t *pCompletions, const char *hints, crossline_color_e color);


/*
 * Paging APIs
 */

// Enable/Disble paging control
CLN_EXPORT int crossline_paging_set (int enable);

// Check paging after print a line, return 1 means quit, 0 means continue
// if you know only one line is printed, just give line_len = 1
CLN_EXPORT int  crossline_paging_check (int line_len);


/* 
 * Cursor APIs
 */

// Get screen rows and columns
CLN_EXPORT void crossline_screen_get (int *pRows, int *pCols);

// Clear current screen
CLN_EXPORT void crossline_screen_clear (void);

// Get cursor postion (0 based)
//CLN_EXPORT int  crossline_cursor_get (int *pRow, int *pCol);

// Set cursor postion (0 based)
//CLN_EXPORT void crossline_cursor_set (int row, int col);

// Move cursor with row and column offset, row_off>0 move up row_off lines, <0 move down abs(row_off) lines
// =0 no move for row, similar with col_off
CLN_EXPORT void crossline_cursor_move (int row_off, int col_off);

// Hide or show cursor
//CLN_EXPORT void crossline_cursor_hide (int bHide);


/* 
 * Color APIs
 */

// Set text color, CROSSLINE_COLOR_DEFAULT will revert to default setting
// `\t` is not supported in Linux terminal, same below. Don't use `\n` in Linux terminal, same below.
CLN_EXPORT void crossline_color_set (crossline_color_e color);

// Set default prompt color
CLN_EXPORT void crossline_prompt_color_set (crossline_color_e color);

#ifdef __cplusplus
}
#endif

#endif /* __CROSSLINE_H */