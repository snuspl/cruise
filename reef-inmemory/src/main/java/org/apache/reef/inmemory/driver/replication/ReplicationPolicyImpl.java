package org.apache.reef.inmemory.driver.replication;

import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.replication.*;
import org.apache.reef.inmemory.driver.service.MetaServerParameters;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ReplicationPolicyImpl implements ReplicationPolicy {

  private static final Logger LOG = Logger.getLogger(ReplicationPolicyImpl.class.getName());
  private static final Pattern sizePattern = Pattern.compile("([\\d.]+)([KMG])?", Pattern.CASE_INSENSITIVE);

  private Rules rules;

  public ReplicationPolicyImpl(final Rules rules) {
    this.rules = rules;
  }

  @Inject
  public ReplicationPolicyImpl(final @Parameter(MetaServerParameters.ReplicationRulesJson.class) String rulesString) {
    try {
      LOG.log(Level.FINE, "Applying rules: "+rulesString);
      rules = AvroReplicationSerializer.fromString(rulesString);
    } catch (IOException e) {
      throw new BindException("Could not bind Replication Rules", e);
    }
  }

  @Override
  public Action getReplicationAction(String path, FileMeta metadata) {
    for (Rule rule : rules.getRules()) {
      if (matches(rule, path, metadata)) {
        return rule.getAction();
      }
    }

    return rules.getDefault$();
  }

  @Override
  public boolean isBroadcast(Action action) {
    return action.getFactor() < 0;
  }

  private boolean matches(Rule rule, String path, FileMeta metadata) {
    if (rule.getConditions() == null || rule.getConditions().size() == 0 ||
            !"path".equals(rule.getConditions().get(0).getType().toString())) {
      LOG.log(Level.SEVERE, "Malformed rule "+rule);
      return false;
    }

    for (Condition condition : rule.getConditions()) {
      if (!matches(condition, path, metadata)) {
        return false;
      }
    }

    return true;
  }

  private boolean matches(Condition condition, String path, FileMeta metadata) {
    if ("path".equals(condition.getType().toString())) {
      // TODO: should we check the metadata to tell apart files from directories?

      final Path matchPath = new Path(path);
      final Path conditionPath = new Path(condition.getOperand().toString());
      if ("exact".equals(condition.getOperator().toString())) {
        LOG.log(Level.INFO, "1, "+matchPath+" =? "+conditionPath);
        return matchPath.equals(conditionPath);
      } else if ("recursive".equals(condition.getOperator().toString())) {
        final int matchDepth = matchPath.depth();
        final int conditionDepth = conditionPath.depth();
        if (matchPath.equals(conditionPath)) {
          LOG.log(Level.INFO, "2");
          return true;
        } else if (conditionDepth >= matchDepth) {
          LOG.log(Level.INFO, "3");
          return false;
        } else {
          Path matchAtConditionDepth = matchPath;
          for (int i = 0; i < matchDepth - conditionDepth; i++) {
            matchAtConditionDepth = matchAtConditionDepth.getParent();
          }
          LOG.log(Level.INFO, "4, "+matchAtConditionDepth+" =? "+conditionPath);
          return matchAtConditionDepth.equals(conditionPath);
        }
      }
    } else if ("size".equals(condition.getType().toString())) {

      final int operand;
      try {
        operand = parseSize(condition.getOperand().toString());
      } catch (NumberFormatException e) {
        return false;
      }
      final String operator = condition.getOperator().toString();
      final long size = metadata.getFileSize();

      LOG.log(Level.INFO, "operator "+operator+", operand "+operand+", size "+size);

      if ("lt".equals(operator)) {
        return size < operand;
      } else if ("leq".equals(operator)) {
        return size <= operand;
      } else if ("gt".equals(operator)) {
        return size > operand;
      } else if ("geq".equals(operator)) {
        return size >= operand;
      } else {
        return false;
      }
    }

    LOG.log(Level.INFO, "5");
    return false;
  }

  private int parseSize(String s) throws NumberFormatException {
    final Matcher matcher = sizePattern.matcher(s);
    if (matcher.find()) {
      final int number = Integer.parseInt(matcher.group(1));
      final int multiplier;

      if (matcher.group(2) == null) {
        multiplier = 1;
      } else {
        final String unit = matcher.group(2).toUpperCase();
        if ("K".equals(unit)) {
          multiplier = 1024;
        } else if ("M".equals(unit)) {
          multiplier = 1024 * 1024;
        } else if ("G".equals(unit)) {
          multiplier = 1024 * 1024 * 1024;
        } else {
          throw new NumberFormatException("Unknown unit " + unit);
        }
      }
      return number * multiplier;
    } else {
      throw new NumberFormatException("Could not parse size "+s);
    }
  }

  @Override
  public void setRules(Rules rules) {
    this.rules = rules;
  }

  @Override
  public void setRules(String rulesString) throws IOException {
    this.rules = AvroReplicationSerializer.fromString(rulesString);
  }
}
