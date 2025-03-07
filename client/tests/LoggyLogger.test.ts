import { LoggyLogger } from "../src/LoggyLogger";

describe("LoggyLogger", () => {
  it("should log a messgae", () => {
    LoggyLogger.log("INFO", "Test log message", { key: "value" });
  });
});
