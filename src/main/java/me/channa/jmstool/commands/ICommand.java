package me.channa.jmstool.commands;

import com.beust.jcommander.JCommander;

import javax.jms.JMSException;
import javax.naming.NamingException;

public interface ICommand {

    public void execute(JCommander jCommander) throws NamingException, JMSException;
}
