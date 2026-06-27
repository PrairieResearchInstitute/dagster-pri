If you make a commit, follow conventional commits and add a trailer:
`Assisted-by: <harness>:<model>`, where `<harness>` is the current agent harness
(like ClaudeCode), and `<model>` is the AI model (Like claude-opus-4.8). You
don't need to add a coauthored-by Claude when you have this.

# Dependency Management
We use uv to manage dependencies. Always us `uv run` to execute any python code.
