import { BaseWorker, type JobMessage } from "./base-worker.js";
import { ffmpeg } from "./utils.js";

class ThumbnailWorker extends BaseWorker {
  constructor() {
    super({ queue: "thumbnail", jobType: "thumbnail" });
  }

  protected async processMessage({ videoId, originalPath }: JobMessage): Promise<string> {
    const out = `storage/${videoId}.jpg`;
    // grab one frame ~1s in
    await ffmpeg(["-y", "-i", originalPath, "-ss", "00:00:01", "-vframes", "1", out]);
    return out;
  }
}

new ThumbnailWorker().start().catch((err) => {
  console.error(err);
  process.exit(1);
});
