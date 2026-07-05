import { BaseWorker, type JobMessage } from "./base-worker.js";
import { ffmpeg } from "./utils.js";

class TranscodeWorker extends BaseWorker {
  constructor() {
    super({ queue: "transcode", jobType: "transcode" });
  }

  protected async processMessage({ videoId, originalPath }: JobMessage): Promise<string> {
    const out = `storage/${videoId}-480.mp4`;
    await ffmpeg(["-y", "-i", originalPath, "-vf", "scale=-2:480", out]);
    return out;
  }
}

new TranscodeWorker().start().catch((err) => {
  console.error(err);
  process.exit(1);
});
