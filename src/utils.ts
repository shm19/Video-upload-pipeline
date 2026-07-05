import { spawn } from "node:child_process";

// Run a child process, resolve on exit 0, reject otherwise. Generic so it
// works for ffmpeg, whisper-cli, or any CLI tool a worker shells out to.
export function run(cmd: string, args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: "inherit" });
    p.on("error", reject); // e.g. binary not found
    p.on("close", (code) =>
      code === 0 ? resolve() : reject(new Error(`${cmd} exited ${code}`)),
    );
  });
}

// Convenience wrapper for ffmpeg specifically.
export function ffmpeg(args: string[]): Promise<void> {
  return run("ffmpeg", args);
}
