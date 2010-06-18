/*
 * Copyright (C) 2009 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Provides a DTP-backed implementation of the {@code edu.upenn.cis.orchestra.sql} package. 
 * <p>
 * Generally, the classes in this package are allowed to access eachother's backing DTP objects, 
 * which would making piecemeal porting to a non-DTP backed {@code edu.upenn.cis.orchestra.sql}
 * implementation impossible. If piecemeal porting is desired, one would have to start by removing, or making private, the DTP objects' accessors. 
 */
package edu.upenn.cis.orchestra.sql.dtp;