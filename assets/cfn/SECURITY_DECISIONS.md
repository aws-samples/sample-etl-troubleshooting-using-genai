# Security decisions

## teampolicy.json — participant policy posture

This policy is attached to the Workshop Studio `participantRole` (see
`contentspec.yaml`). It grants participants the permissions needed to complete
every lab in an ephemeral, event-scoped AWS account.

### Mitigated: privilege-escalation paths

The following escalation vectors were scoped down rather than accepted:

- **`iam:PassRole`** was removed from the broad `IAMManagement` statement and
  moved to a dedicated `PassRoleToWorkshopServices` statement gated by an
  `iam:PassedToService` condition. Roles can only be passed to the services the
  workshop actually uses (Lambda, EC2, Glue, SageMaker, MWAA / MWAA Serverless,
  CodeBuild, CloudFormation, Bedrock, Bedrock AgentCore). This closes the
  "pass any role to any service" path (PE2/PE4).
- **`sts:AssumeRole`** was narrowed from `Resource: "*"` to
  `arn:aws:iam::${aws:PrincipalAccount}:role/*`, restricting assumption to roles
  within the participant's own account and eliminating cross-account lateral
  movement (PE3). `sts:GetCallerIdentity` remains unrestricted (read-only).

### Accepted risk: service-level wildcards

**Finding:** Several statements grant service-level wildcards (e.g. `es:*`,
`sagemaker:*`, `s3:*`, `lambda:*`, `ec2:*`, `glue:*`, `cloudformation:*`,
`bedrock:*`, `cognito-idp:*`, `ecr:*`, `codebuild:*`) with `Resource: "*"`.

**Justification:** Workshop participants interactively create and manage these
resources from the SageMaker notebook and the console. Resource names are
generated dynamically at runtime, so scoping each action to specific ARNs is not
feasible without breaking the labs.

**Mitigation:** Workshop Studio ephemeral accounts are destroyed after the event
concludes. No persistent data or long-lived credentials exist beyond the event
lifecycle. The escalation paths that would let a participant break *out* of this
permission set (unrestricted `PassRole` and `AssumeRole`) have been scoped as
described above.
