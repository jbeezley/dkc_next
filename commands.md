Using with aws cli
==================

aws --no-sign-request --endpoint-url http://localhost:9000 s3 ls s3://test/
aws --no-sign-request --endpoint-url http://localhost:9000 s3 cp --metadata foo=bar log.txt s3://test/

Metadata search
===============

```
http -v GET :5006/search filter='content-type=application/tiff' limit=1
```

