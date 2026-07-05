import { BaseWorker, type JobMessage } from "./base-worker.js";
import { ffmpeg, run } from "./utils.js";

class CaptionWorker extends BaseWorker {
  constructor() {
    super({ queue: "caption", jobType: "caption" });
  }

  protected async processMessage({ videoId, originalPath }: JobMessage): Promise<string> {
    const wav = `storage/${videoId}.wav`;
    // whisper wants 16kHz mono
    await ffmpeg(["-y", "-i", originalPath, "-ar", "16000", "-ac", "1", wav]);
    await run("whisper", [
      wav,
      "--model",
      "tiny.en",
      "--output_format",
      "vtt",
      "--output_dir",
      "storage",
    ]);
    return `storage/${videoId}.vtt`; // whisper names output after the input basename
  }
}

new CaptionWorker().start().catch((err) => {
  console.error(err);
  process.exit(1);
});
