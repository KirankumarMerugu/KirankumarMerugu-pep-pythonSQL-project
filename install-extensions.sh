
#!/bin/bash
if [ -f "requirements.txt" ]; then
pip install -r requirements.txt
fi
mkdir -p .vscode
cat << 'EOF' >.vscode/settings.json
{
    "python.testing.unittestArgs":["-v","-s","./src/test","-p","*test*.py"],
    "python.testing.pytestEnabled": false,
    "python.testing.unittestEnabled":true,
    "extensions.ignoreRecommendations": true
}
EOF
cat << 'EOF' > .git/hooks/post-commit
#!/bin/bash
git push
git log -1 --shortstat > history_log.txt
EOF
chmod +x .git/hooks/post-commit
code --install-extension revaturePro.revature-python-labs
if [ -f "src/main/lab.py" ]; then
code -r src/main/lab.py
elif [ -f "src/main/app.py" ]; then
code -r src/main/app.py
else
find src/main -type f -name "*.py" | while read file; do
code -r "$file"
done
fi