import { action, cron, v } from "@chimpbase/runtime";
import { spawn } from "node:child_process";

const ATTACHMENT_BUCKET = "attachments";

export const uploadAttachment = action({
  name: "attachments.upload",
  args: v.object({
    orderId: v.string(),
    filename: v.string(),
    body: v.string(),
    contentType: v.string().optional(),
  }),
  async handler(ctx, input) {
    const key = `${input.orderId}/${input.filename}`;
    const result = await ctx.blobs.put(
      ATTACHMENT_BUCKET,
      key,
      new TextEncoder().encode(input.body),
      {
        contentType: input.contentType ?? "text/plain",
        metadata: { orderId: input.orderId },
      },
    );
    ctx.log.info("attachment uploaded", { orderId: input.orderId, key, size: result.size });
    return { key, size: result.size, etag: result.etag };
  },
});

export const downloadAttachment = action({
  name: "attachments.download",
  args: v.object({ orderId: v.string(), filename: v.string() }),
  async handler(ctx, input) {
    const obj = await ctx.blobs.get(ATTACHMENT_BUCKET, `${input.orderId}/${input.filename}`);
    if (!(obj !== null)) return null;
    const text = await new Response(obj.body).text();
    return { text, size: obj.size, etag: obj.etag };
  },
});

export const listAttachments = action({
  name: "attachments.list",
  args: v.object({ orderId: v.string() }),
  async handler(ctx, input) {
    return await ctx.blobs.list(ATTACHMENT_BUCKET, { prefix: `${input.orderId}/` });
  },
});

export const backupAttachments = cron(
  "attachments.backup",
  "*/5 * * * *",
  async (ctx) => {
    const source = process.env.ATTACHMENTS_ROOT;
    const target = process.env.ATTACHMENTS_BACKUP_ROOT;
    if (!(source !== undefined && source.length > 0) || !(target !== undefined && target.length > 0)) {
      ctx.log.debug("attachments backup skipped (ATTACHMENTS_ROOT or ATTACHMENTS_BACKUP_ROOT unset)");
      return;
    }
    await new Promise<void>((resolveRun, rejectRun) => {
      const proc = spawn("rsync", ["-a", "--delete", `${source}/`, `${target}/`], {
        stdio: ["ignore", "inherit", "inherit"],
      });
      proc.on("error", rejectRun);
      proc.on("exit", (code) => {
        if (code === 0) resolveRun();
        else rejectRun(new Error(`rsync exited with code ${code}`));
      });
    });
    ctx.log.info("attachments backup complete", { source, target });
  },
);
