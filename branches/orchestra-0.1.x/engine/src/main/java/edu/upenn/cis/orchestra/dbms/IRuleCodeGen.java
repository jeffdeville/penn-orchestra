/*
 * Copyright (C) 2010 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS of ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.dbms;

import java.util.List;

/**
 * Basic interface to DB-specific code generation
 * @author zives
 *
 */
public interface IRuleCodeGen {
	public enum UPDATE_TYPE {CLEAR_AND_COPY, DELETE_FROM_HEAD, OTHER};
    
	//public String toQuery(Map<ScField, String> typesMap);
	
    public List<String> getCode(UPDATE_TYPE u, int curIterCnt);

    public List<String> getCode(List<String> existing, UPDATE_TYPE u, int curIterCnt);
}
