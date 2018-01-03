package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/progress/termstatus"
	"github.com/restic/restic/internal/restic"
)

var cmdBackup = &cobra.Command{
	Use:   "backup [flags] FILE/DIR [FILE/DIR] ...",
	Short: "Create a new backup of files and/or directories",
	Long: `
The "backup" command creates a new snapshot and saves the files and directories
given as the arguments.
`,
	PreRun: func(cmd *cobra.Command, args []string) {
		if backupOptions.Hostname == "" {
			hostname, err := os.Hostname()
			if err != nil {
				debug.Log("os.Hostname() returned err: %v", err)
				return
			}
			backupOptions.Hostname = hostname
		}
	},
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if backupOptions.Stdin && backupOptions.FilesFrom == "-" {
			return errors.Fatal("cannot use both `--stdin` and `--files-from -`")
		}

		return runBackup(backupOptions, globalOptions, args)
	},
}

// BackupOptions bundles all options for the backup command.
type BackupOptions struct {
	Parent           string
	Force            bool
	Excludes         []string
	ExcludeFiles     []string
	ExcludeOtherFS   bool
	ExcludeIfPresent []string
	ExcludeCaches    bool
	Stdin            bool
	StdinFilename    string
	Tags             []string
	Hostname         string
	FilesFrom        string
	TimeStamp        string
	WithAtime        bool
}

var backupOptions BackupOptions

func init() {
	cmdRoot.AddCommand(cmdBackup)

	f := cmdBackup.Flags()
	f.StringVar(&backupOptions.Parent, "parent", "", "use this parent snapshot (default: last snapshot in the repo that has the same target files/directories)")
	f.BoolVarP(&backupOptions.Force, "force", "f", false, `force re-reading the target files/directories (overrides the "parent" flag)`)
	f.StringArrayVarP(&backupOptions.Excludes, "exclude", "e", nil, "exclude a `pattern` (can be specified multiple times)")
	f.StringArrayVar(&backupOptions.ExcludeFiles, "exclude-file", nil, "read exclude patterns from a `file` (can be specified multiple times)")
	f.BoolVarP(&backupOptions.ExcludeOtherFS, "one-file-system", "x", false, "exclude other file systems")
	f.StringArrayVar(&backupOptions.ExcludeIfPresent, "exclude-if-present", nil, "takes filename[:header], exclude contents of directories containing filename (except filename itself) if header of that file is as provided (can be specified multiple times)")
	f.BoolVar(&backupOptions.ExcludeCaches, "exclude-caches", false, `excludes cache directories that are marked with a CACHEDIR.TAG file`)
	f.BoolVar(&backupOptions.Stdin, "stdin", false, "read backup from stdin")
	f.StringVar(&backupOptions.StdinFilename, "stdin-filename", "stdin", "file name to use when reading from stdin")
	f.StringArrayVar(&backupOptions.Tags, "tag", nil, "add a `tag` for the new snapshot (can be specified multiple times)")
	f.StringVar(&backupOptions.Hostname, "hostname", "", "set the `hostname` for the snapshot manually. To prevent an expensive rescan use the \"parent\" flag")
	f.StringVar(&backupOptions.FilesFrom, "files-from", "", "read the files to backup from file (can be combined with file args)")
	f.StringVar(&backupOptions.TimeStamp, "time", "", "time of the backup (ex. '2012-11-01 22:08:41') (default: now)")
	f.BoolVar(&backupOptions.WithAtime, "with-atime", false, "store the atime for all files and directories")
}

// filterExisting returns a slice of all existing items, or an error if no
// items exist at all.
func filterExisting(items []string) (result []string, err error) {
	for _, item := range items {
		_, err := fs.Lstat(item)
		if err != nil && os.IsNotExist(errors.Cause(err)) {
			Warnf("%v does not exist, skipping\n", item)
			continue
		}

		result = append(result, item)
	}

	if len(result) == 0 {
		return nil, errors.Fatal("all target directories/files do not exist")
	}

	return
}

// readFromFile will read all lines from the given filename and write them to a
// string array, if filename is empty readFromFile returns and empty string
// array. If filename is a dash (-), readFromFile will read the lines from
// the standard input.
func readLinesFromFile(filename string) ([]string, error) {
	if filename == "" {
		return nil, nil
	}

	var r io.Reader = os.Stdin
	if filename != "-" {
		f, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}

	var lines []string

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// ignore empty lines
		if line == "" {
			continue
		}
		// strip comments
		if strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

func runBackup(opts BackupOptions, gopts GlobalOptions, args []string) error {
	if opts.FilesFrom == "-" && gopts.password == "" {
		return errors.Fatal("unable to read password from stdin when data is to be read from stdin, use --password-file or $RESTIC_PASSWORD")
	}

	fromfile, err := readLinesFromFile(opts.FilesFrom)
	if err != nil {
		return err
	}

	// merge files from files-from into normal args so we can reuse the normal
	// args checks and have the ability to use both files-from and args at the
	// same time
	args = append(args, fromfile...)
	if len(args) == 0 {
		return errors.Fatal("nothing to backup, please specify target files/dirs")
	}

	target := args
	target, err = filterExisting(target)
	if err != nil {
		return err
	}

	// rejectFuncs collect functions that can reject items from the backup
	var rejectFuncs []RejectFunc

	// allowed devices
	if opts.ExcludeOtherFS {
		f, err := rejectByDevice(target)
		if err != nil {
			return err
		}
		rejectFuncs = append(rejectFuncs, f)
	}

	// add patterns from file
	if len(opts.ExcludeFiles) > 0 {
		opts.Excludes = append(opts.Excludes, readExcludePatternsFromFiles(opts.ExcludeFiles)...)
	}

	if len(opts.Excludes) > 0 {
		rejectFuncs = append(rejectFuncs, rejectByPattern(opts.Excludes))
	}

	if opts.ExcludeCaches {
		opts.ExcludeIfPresent = append(opts.ExcludeIfPresent, "CACHEDIR.TAG:Signature: 8a477f597d28d172789f06886806bc55")
	}

	for _, spec := range opts.ExcludeIfPresent {
		f, err := rejectIfPresent(spec)
		if err != nil {
			return err
		}

		rejectFuncs = append(rejectFuncs, f)
	}

	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	lock, err := lockRepo(repo)
	defer unlockRepo(lock)
	if err != nil {
		return err
	}

	// exclude restic cache
	if repo.Cache != nil {
		f, err := rejectResticCache(repo)
		if err != nil {
			return err
		}

		rejectFuncs = append(rejectFuncs, f)
	}

	err = repo.LoadIndex(gopts.ctx)
	if err != nil {
		return err
	}

	var parentSnapshotID *restic.ID

	// Force using a parent
	if !opts.Force && opts.Parent != "" {
		id, err := restic.FindSnapshot(repo, opts.Parent)
		if err != nil {
			return errors.Fatalf("invalid id %q: %v", opts.Parent, err)
		}

		parentSnapshotID = &id
	}

	// Find last snapshot to set it as parent, if not already set
	if !opts.Force && parentSnapshotID == nil {
		id, err := restic.FindLatestSnapshot(gopts.ctx, repo, target, []restic.TagList{}, opts.Hostname)
		if err == nil {
			parentSnapshotID = &id
		} else if err != restic.ErrNoSnapshotFound {
			return err
		}
	}

	if parentSnapshotID != nil {
		Verbosef("using parent snapshot %v\n", parentSnapshotID.Str())
	}

	selectFilter := func(item string, fi os.FileInfo) bool {
		for _, reject := range rejectFuncs {
			if reject(item, fi) {
				return false
			}
		}
		return true
	}

	timeStamp := time.Now()
	if opts.TimeStamp != "" {
		timeStamp, err = time.Parse(TimeFormat, opts.TimeStamp)
		if err != nil {
			return errors.Fatalf("error in time option: %v\n", err)
		}
	}

	var targetFS fs.FS = fs.Local{}
	if opts.Stdin {
		targetFS = &fs.Reader{
			ModTime:    timeStamp,
			Name:       opts.StdinFilename,
			Mode:       0644,
			ReadCloser: os.Stdin,
		}
	}

	term := termstatus.New(gopts.ctx, os.Stdout)
	AddCleanupHandler(term.Finish)

	arch := archiver.NewNewArchiver(repo, targetFS)
	arch.Select = selectFilter
	arch.WithAtime = opts.WithAtime

	var stats archiver.ItemStats
	var cur []string

	arch.StartItem = func(item string) {
		if strings.HasSuffix(item, "/") {
			return
		}
		// Verbosef("  %v [start]\n", item)
		if len(cur) > 3 {
			cur = cur[1:4]
		}
		cur = append(cur, item)
		term.SetStatus(append([]string{fmt.Sprintf("lines: %d", len(cur))}, cur...))
	}

	arch.CompleteItem = func(item string, previous, current *restic.Node, s archiver.ItemStats) {
		// stats.Add(s)
		// if item == "/" {
		// 	return
		// }

		// if previous == nil {
		// 	term.Printf("+ %v %v\n", item, s)
		// 	return
		// }

		// if current != nil && previous.Equals(*current) {
		// 	term.Printf("  %v\n", item)
		// 	return
		// }

		// term.Printf("M %v %v\n", item, s)
	}

	if parentSnapshotID == nil {
		parentSnapshotID = &restic.ID{}
	}

	snapshotOpts := archiver.Options{
		Excludes:       opts.Excludes,
		Tags:           opts.Tags,
		Time:           timeStamp,
		Hostname:       opts.Hostname,
		ParentSnapshot: *parentSnapshotID,
	}

	_, id, err := arch.Snapshot(gopts.ctx, target, snapshotOpts)
	if err != nil {
		return err
	}

	err = term.Finish()
	if err != nil {
		Warnf("error: %v", err)
	}

	Verbosef("snapshot %s saved\n", id.Str())
	Verbosef("added %v data and %v metadata\n", formatBytes(stats.DataSize), formatBytes(stats.TreeSize))

	return nil
}

func readExcludePatternsFromFiles(excludeFiles []string) []string {
	var excludes []string
	for _, filename := range excludeFiles {
		err := func() (err error) {
			file, err := fs.Open(filename)
			if err != nil {
				return err
			}
			defer func() {
				// return pre-close error if there was one
				if errClose := file.Close(); err == nil {
					err = errClose
				}
			}()

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())

				// ignore empty lines
				if line == "" {
					continue
				}

				// strip comments
				if strings.HasPrefix(line, "#") {
					continue
				}

				line = os.ExpandEnv(line)
				excludes = append(excludes, line)
			}
			return scanner.Err()
		}()
		if err != nil {
			Warnf("error reading exclude patterns: %v:", err)
			return nil
		}
	}
	return excludes
}
